\set ECHO all
SET client_min_messages = warning;
SET datestyle TO ISO;
SET client_encoding = utf8;

-- prepare
select current_database();
-- select current_user;

select rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit from pg_authid where rolname = 'audit_user' order by rolname;

DROP USER IF EXISTS audit_user;
DROP DATABASE IF EXISTS audit_database;

CREATE USER audit_user WITH SUPERUSER CREATEDB LOGIN ENCRYPTED PASSWORD 'audit_user';
CREATE DATABASE audit_database;

select rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit from pg_authid where rolname = 'audit_user' order by rolname;

-- create table
\c audit_database audit_user
create table tbl_test(f1 number, f2 timestamp default now(), f3 int) ;
create table tbl_test0 as select * from tbl_test;

create view tbl_test_v as select * from tbl_test;
create materialized view tbl_test_mv as select * from tbl_test;

create view tbl_test0_v as select * from tbl_test0;
create materialized view tbl_test0_mv as select * from tbl_test0;

create schema sc_test;
create table sc_test.tbl_test(f1 number, f2 timestamp default now(), f3 int) ;
create table sc_test.tbl_test0 as select * from sc_test.tbl_test;

create view sc_test.tbl_test_v as select * from sc_test.tbl_test;
create materialized view sc_test.tbl_test_mv as select * from sc_test.tbl_test;

create view sc_test.tbl_test0_v as select * from sc_test.tbl_test0;
create materialized view sc_test.tbl_test0_mv as select * from sc_test.tbl_test0;

-- create and check audit rules 
\c audit_database audit_admin
audit all;
audit all by audit_user;
audit all on default;

audit all on tbl_test;
audit all on tbl_test_v;
audit all on view tbl_test_v;
audit all on tbl_test_mv;
audit all on view tbl_test_mv;
audit all on materialized view tbl_test_mv;

audit all on tbl_test0;
audit all on tbl_test0_v;
audit all on view tbl_test0_v;
audit all on tbl_test0_mv;
audit all on view tbl_test0_mv;
audit all on materialized view tbl_test0_mv;

audit all on sc_test.tbl_test;
audit all on sc_test.tbl_test_v;
audit all on view sc_test.tbl_test_v;
audit all on sc_test.tbl_test_mv;
audit all on view sc_test.tbl_test_mv;
audit all on materialized view sc_test.tbl_test_mv;

audit all on sc_test.tbl_test0;
audit all on sc_test.tbl_test0_v;
audit all on view sc_test.tbl_test0_v;
audit all on sc_test.tbl_test0_mv;
audit all on view sc_test.tbl_test0_mv;
audit all on materialized view sc_test.tbl_test0_mv;

select * from pg_audit_obj_conf_detail order by auditor, object_class, object_desc, action_name, action_mode;
select * from pg_audit_user_conf_detail order by auditor, user_name, action_name, action_mode;
select * from pg_audit_stmt_conf_detail order by auditor, action_name, action_mode;
select * from pg_audit_obj_def_opts_detail order by auditor, action_name;

noaudit all WHENEVER NOT SUCCESSFUL;
noaudit all by audit_user WHENEVER NOT SUCCESSFUL;
noaudit all on default WHENEVER NOT SUCCESSFUL;

noaudit all on tbl_test WHENEVER NOT SUCCESSFUL;
noaudit all on tbl_test_v WHENEVER NOT SUCCESSFUL;
noaudit all on view tbl_test_v WHENEVER NOT SUCCESSFUL;
noaudit all on tbl_test_mv WHENEVER NOT SUCCESSFUL;
noaudit all on view tbl_test_mv WHENEVER NOT SUCCESSFUL;
noaudit all on materialized view tbl_test_mv WHENEVER NOT SUCCESSFUL;

noaudit all on tbl_test0 WHENEVER NOT SUCCESSFUL;
noaudit all on tbl_test0_v WHENEVER NOT SUCCESSFUL;
noaudit all on view tbl_test0_v WHENEVER NOT SUCCESSFUL;
noaudit all on tbl_test0_mv WHENEVER NOT SUCCESSFUL;
noaudit all on view tbl_test0_mv WHENEVER NOT SUCCESSFUL;
noaudit all on materialized view tbl_test0_mv WHENEVER NOT SUCCESSFUL;

noaudit all on sc_test.tbl_test WHENEVER NOT SUCCESSFUL;
noaudit all on sc_test.tbl_test_v WHENEVER NOT SUCCESSFUL;
noaudit all on view sc_test.tbl_test_v WHENEVER NOT SUCCESSFUL;
noaudit all on sc_test.tbl_test_mv WHENEVER NOT SUCCESSFUL;
noaudit all on view sc_test.tbl_test_mv WHENEVER NOT SUCCESSFUL;
noaudit all on materialized view sc_test.tbl_test_mv WHENEVER NOT SUCCESSFUL;

noaudit all on sc_test.tbl_test0 WHENEVER NOT SUCCESSFUL;
noaudit all on sc_test.tbl_test0_v WHENEVER NOT SUCCESSFUL;
noaudit all on view sc_test.tbl_test0_v WHENEVER NOT SUCCESSFUL;
noaudit all on sc_test.tbl_test0_mv WHENEVER NOT SUCCESSFUL;
noaudit all on view sc_test.tbl_test0_mv WHENEVER NOT SUCCESSFUL;
noaudit all on materialized view sc_test.tbl_test0_mv WHENEVER NOT SUCCESSFUL;

select * from pg_audit_obj_conf_detail order by auditor, object_class, object_desc, action_name, action_mode;
select * from pg_audit_user_conf_detail order by auditor, user_name, action_name, action_mode;
select * from pg_audit_stmt_conf_detail order by auditor, action_name, action_mode;
select * from pg_audit_obj_def_opts_detail order by auditor, action_name;

noaudit all WHENEVER SUCCESSFUL;
noaudit all by audit_user WHENEVER SUCCESSFUL;
noaudit all on default WHENEVER SUCCESSFUL;

noaudit all on tbl_test WHENEVER SUCCESSFUL;
noaudit all on tbl_test_v WHENEVER SUCCESSFUL;
noaudit all on view tbl_test_v WHENEVER SUCCESSFUL;
noaudit all on tbl_test_mv WHENEVER SUCCESSFUL;
noaudit all on view tbl_test_mv WHENEVER SUCCESSFUL;
noaudit all on materialized view tbl_test_mv WHENEVER SUCCESSFUL;

noaudit all on tbl_test0 WHENEVER SUCCESSFUL;
noaudit all on tbl_test0_v WHENEVER SUCCESSFUL;
noaudit all on view tbl_test0_v WHENEVER SUCCESSFUL;
noaudit all on tbl_test0_mv WHENEVER SUCCESSFUL;
noaudit all on view tbl_test0_mv WHENEVER SUCCESSFUL;
noaudit all on materialized view tbl_test0_mv WHENEVER SUCCESSFUL;

noaudit all on sc_test.tbl_test WHENEVER SUCCESSFUL;
noaudit all on sc_test.tbl_test_v WHENEVER SUCCESSFUL;
noaudit all on view sc_test.tbl_test_v WHENEVER SUCCESSFUL;
noaudit all on sc_test.tbl_test_mv WHENEVER SUCCESSFUL;
noaudit all on view sc_test.tbl_test_mv WHENEVER SUCCESSFUL;
noaudit all on materialized view sc_test.tbl_test_mv WHENEVER SUCCESSFUL;

noaudit all on sc_test.tbl_test0 WHENEVER SUCCESSFUL;
noaudit all on sc_test.tbl_test0_v WHENEVER SUCCESSFUL;
noaudit all on view sc_test.tbl_test0_v WHENEVER SUCCESSFUL;
noaudit all on sc_test.tbl_test0_mv WHENEVER SUCCESSFUL;
noaudit all on view sc_test.tbl_test0_mv WHENEVER SUCCESSFUL;
noaudit all on materialized view sc_test.tbl_test0_mv WHENEVER SUCCESSFUL;

select * from pg_audit_obj_conf_detail order by auditor, object_class, object_desc, action_name, action_mode;
select * from pg_audit_user_conf_detail order by auditor, user_name, action_name, action_mode;
select * from pg_audit_stmt_conf_detail order by auditor, action_name, action_mode;
select * from pg_audit_obj_def_opts_detail order by auditor, action_name;

noaudit all;
noaudit all by audit_user;
noaudit all on default;

noaudit all on tbl_test;
noaudit all on tbl_test_v;
noaudit all on view tbl_test_v;
noaudit all on tbl_test_mv;
noaudit all on view tbl_test_mv;
noaudit all on materialized view tbl_test_mv;

noaudit all on tbl_test0;
noaudit all on tbl_test0_v;
noaudit all on view tbl_test0_v;
noaudit all on tbl_test0_mv;
noaudit all on view tbl_test0_mv;
noaudit all on materialized view tbl_test0_mv;

noaudit all on sc_test.tbl_test;
noaudit all on sc_test.tbl_test_v;
noaudit all on view sc_test.tbl_test_v;
noaudit all on sc_test.tbl_test_mv;
noaudit all on view sc_test.tbl_test_mv;
noaudit all on materialized view sc_test.tbl_test_mv;

noaudit all on sc_test.tbl_test0;
noaudit all on sc_test.tbl_test0_v;
noaudit all on view sc_test.tbl_test0_v;
noaudit all on sc_test.tbl_test0_mv;
noaudit all on view sc_test.tbl_test0_mv;
noaudit all on materialized view sc_test.tbl_test0_mv;

select * from pg_audit_obj_conf_detail order by auditor, object_class, object_desc, action_name, action_mode;
select * from pg_audit_user_conf_detail order by auditor, user_name, action_name, action_mode;
select * from pg_audit_stmt_conf_detail order by auditor, action_name, action_mode;
select * from pg_audit_obj_def_opts_detail order by auditor, action_name;

audit all WHENEVER NOT SUCCESSFUL;
audit all by audit_user WHENEVER NOT SUCCESSFUL;
audit all on default WHENEVER NOT SUCCESSFUL;

audit all on tbl_test WHENEVER NOT SUCCESSFUL;
audit all on tbl_test_v WHENEVER NOT SUCCESSFUL;
audit all on view tbl_test_v WHENEVER NOT SUCCESSFUL;
audit all on tbl_test_mv WHENEVER NOT SUCCESSFUL;
audit all on view tbl_test_mv WHENEVER NOT SUCCESSFUL;
audit all on materialized view tbl_test_mv WHENEVER NOT SUCCESSFUL;

audit all on tbl_test0 WHENEVER NOT SUCCESSFUL;
audit all on tbl_test0_v WHENEVER NOT SUCCESSFUL;
audit all on view tbl_test0_v WHENEVER NOT SUCCESSFUL;
audit all on tbl_test0_mv WHENEVER NOT SUCCESSFUL;
audit all on view tbl_test0_mv WHENEVER NOT SUCCESSFUL;
audit all on materialized view tbl_test0_mv WHENEVER NOT SUCCESSFUL;

audit all on sc_test.tbl_test WHENEVER NOT SUCCESSFUL;
audit all on sc_test.tbl_test_v WHENEVER NOT SUCCESSFUL;
audit all on view sc_test.tbl_test_v WHENEVER NOT SUCCESSFUL;
audit all on sc_test.tbl_test_mv WHENEVER NOT SUCCESSFUL;
audit all on view sc_test.tbl_test_mv WHENEVER NOT SUCCESSFUL;
audit all on materialized view sc_test.tbl_test_mv WHENEVER NOT SUCCESSFUL;

audit all on sc_test.tbl_test0 WHENEVER NOT SUCCESSFUL;
audit all on sc_test.tbl_test0_v WHENEVER NOT SUCCESSFUL;
audit all on view sc_test.tbl_test0_v WHENEVER NOT SUCCESSFUL;
audit all on sc_test.tbl_test0_mv WHENEVER NOT SUCCESSFUL;
audit all on view sc_test.tbl_test0_mv WHENEVER NOT SUCCESSFUL;
audit all on materialized view sc_test.tbl_test0_mv WHENEVER NOT SUCCESSFUL;

select * from pg_audit_obj_conf_detail order by auditor, object_class, object_desc, action_name, action_mode;
select * from pg_audit_user_conf_detail order by auditor, user_name, action_name, action_mode;
select * from pg_audit_stmt_conf_detail order by auditor, action_name, action_mode;
select * from pg_audit_obj_def_opts_detail order by auditor, action_name;

audit all WHENEVER SUCCESSFUL;
audit all by audit_user WHENEVER SUCCESSFUL;
audit all on default WHENEVER SUCCESSFUL;

audit all on tbl_test WHENEVER SUCCESSFUL;
audit all on tbl_test_v WHENEVER SUCCESSFUL;
audit all on view tbl_test_v WHENEVER SUCCESSFUL;
audit all on tbl_test_mv WHENEVER SUCCESSFUL;
audit all on view tbl_test_mv WHENEVER SUCCESSFUL;
audit all on materialized view tbl_test_mv WHENEVER SUCCESSFUL;

audit all on tbl_test0 WHENEVER SUCCESSFUL;
audit all on tbl_test0_v WHENEVER SUCCESSFUL;
audit all on view tbl_test0_v WHENEVER SUCCESSFUL;
audit all on tbl_test0_mv WHENEVER SUCCESSFUL;
audit all on view tbl_test0_mv WHENEVER SUCCESSFUL;
audit all on materialized view tbl_test0_mv WHENEVER SUCCESSFUL;

audit all on sc_test.tbl_test WHENEVER SUCCESSFUL;
audit all on sc_test.tbl_test_v WHENEVER SUCCESSFUL;
audit all on view sc_test.tbl_test_v WHENEVER SUCCESSFUL;
audit all on sc_test.tbl_test_mv WHENEVER SUCCESSFUL;
audit all on view sc_test.tbl_test_mv WHENEVER SUCCESSFUL;
audit all on materialized view sc_test.tbl_test_mv WHENEVER SUCCESSFUL;

audit all on sc_test.tbl_test0 WHENEVER SUCCESSFUL;
audit all on sc_test.tbl_test0_v WHENEVER SUCCESSFUL;
audit all on view sc_test.tbl_test0_v WHENEVER SUCCESSFUL;
audit all on sc_test.tbl_test0_mv WHENEVER SUCCESSFUL;
audit all on view sc_test.tbl_test0_mv WHENEVER SUCCESSFUL;
audit all on materialized view sc_test.tbl_test0_mv WHENEVER SUCCESSFUL;

select * from pg_audit_obj_conf_detail order by auditor, object_class, object_desc, action_name, action_mode;
select * from pg_audit_user_conf_detail order by auditor, user_name, action_name, action_mode;
select * from pg_audit_stmt_conf_detail order by auditor, action_name, action_mode;
select * from pg_audit_obj_def_opts_detail order by auditor, action_name;

\c audit_database audit_user
drop schema sc_test cascade;

\c audit_database audit_admin
clean unknown audit;

select * from pg_audit_obj_conf_detail order by auditor, object_class, object_desc, action_name, action_mode;
select * from pg_audit_user_conf_detail order by auditor, user_name, action_name, action_mode;
select * from pg_audit_stmt_conf_detail order by auditor, action_name, action_mode;
select * from pg_audit_obj_def_opts_detail order by auditor, action_name;

clean statement audit;
select * from pg_audit_obj_conf_detail order by auditor, object_class, object_desc, action_name, action_mode;
select * from pg_audit_user_conf_detail order by auditor, user_name, action_name, action_mode;
select * from pg_audit_stmt_conf_detail order by auditor, action_name, action_mode;
select * from pg_audit_obj_def_opts_detail order by auditor, action_name;

clean user audit by audit_user;
select * from pg_audit_obj_conf_detail order by auditor, object_class, object_desc, action_name, action_mode;
select * from pg_audit_user_conf_detail order by auditor, user_name, action_name, action_mode;
select * from pg_audit_stmt_conf_detail order by auditor, action_name, action_mode;
select * from pg_audit_obj_def_opts_detail order by auditor, action_name;

clean object audit on table tbl_test;
select * from pg_audit_obj_conf_detail order by auditor, object_class, object_desc, action_name, action_mode;
select * from pg_audit_user_conf_detail order by auditor, user_name, action_name, action_mode;
select * from pg_audit_stmt_conf_detail order by auditor, action_name, action_mode;
select * from pg_audit_obj_def_opts_detail order by auditor, action_name;

clean object audit on view tbl_test_v;
select * from pg_audit_obj_conf_detail order by auditor, object_class, object_desc, action_name, action_mode;
select * from pg_audit_user_conf_detail order by auditor, user_name, action_name, action_mode;
select * from pg_audit_stmt_conf_detail order by auditor, action_name, action_mode;
select * from pg_audit_obj_def_opts_detail order by auditor, action_name;

clean object audit on materialized view tbl_test_mv;
select * from pg_audit_obj_conf_detail order by auditor, object_class, object_desc, action_name, action_mode;
select * from pg_audit_user_conf_detail order by auditor, user_name, action_name, action_mode;
select * from pg_audit_stmt_conf_detail order by auditor, action_name, action_mode;
select * from pg_audit_obj_def_opts_detail order by auditor, action_name;

clean object audit;
select * from pg_audit_obj_conf_detail order by auditor, object_class, object_desc, action_name, action_mode;
select * from pg_audit_user_conf_detail order by auditor, user_name, action_name, action_mode;
select * from pg_audit_stmt_conf_detail order by auditor, action_name, action_mode;
select * from pg_audit_obj_def_opts_detail order by auditor, action_name;

clean object audit on default;
select * from pg_audit_obj_conf_detail order by auditor, object_class, object_desc, action_name, action_mode;
select * from pg_audit_user_conf_detail order by auditor, user_name, action_name, action_mode;
select * from pg_audit_stmt_conf_detail order by auditor, action_name, action_mode;
select * from pg_audit_obj_def_opts_detail order by auditor, action_name;

clean all audit;
select * from pg_audit_obj_conf_detail order by auditor, object_class, object_desc, action_name, action_mode;
select * from pg_audit_user_conf_detail order by auditor, user_name, action_name, action_mode;
select * from pg_audit_stmt_conf_detail order by auditor, action_name, action_mode;
select * from pg_audit_obj_def_opts_detail order by auditor, action_name;

\c audit_database audit_user
drop table tbl_test cascade;
drop table tbl_test0 cascade;

\c regression audit_user
drop database audit_database;

reset client_min_messages;
reset datestyle;
reset client_encoding;


