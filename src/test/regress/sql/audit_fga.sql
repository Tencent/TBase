\set ECHO all
SET client_min_messages = warning;
SET datestyle TO ISO;
SET client_encoding = utf8;

-- prepare
DROP USER IF EXISTS audit_fga_user;
DROP DATABASE IF EXISTS audit_fga_database;

CREATE USER audit_fga_user WITH SUPERUSER CREATEDB LOGIN ENCRYPTED PASSWORD 'audit_fga_user';
CREATE DATABASE audit_fga_database;

create user audit_sup superuser;
\c audit_fga_database audit_sup;
create extension pg_fga;

-- create table
\c audit_fga_database audit_fga_user
create table foo(idx bigint, str text);
create table bar(idx bigint, str text);

insert into foo values(1, 'a');
insert into bar values(1, 'a');
select * from foo;
select * from bar;

\c audit_fga_database audit_admin

-- add_policy
select add_policy(object_schema:='public', object_name:='foo', audit_columns:='idx',policy_name:='poli', audit_condition:='idx > 1');
select add_policy(object_schema:='public', object_name:='bar', audit_columns:='idx',policy_name:='poli3', audit_condition:='idx > 1', handler_schema:='pg_catalog', handler_module:='remove_valid_fga_policy');
select add_policy(object_schema:='public', object_name:='bar', audit_columns:='idx',policy_name:='poli4', audit_condition:='idx > 1', handler_schema:='pg_catalog', handler_module:='remove_valid_fga_policy');

select * from pg_audit_fga_conf;
select * from pg_audit_fga_conf_detail ;
select *from pg_audit_fga_policy_columns_detail;

-- drop_policy
select drop_policy(object_schema:='public', object_name:='bar', policy_name:='poli4');

select * from pg_audit_fga_conf;
select * from pg_audit_fga_conf_detail ;
select *from pg_audit_fga_policy_columns_detail;

-- disable_policy
select disable_policy(object_schema:='public', object_name:='foo', policy_name:='poli3');

select * from pg_audit_fga_conf;
select * from pg_audit_fga_conf_detail ;
select *from pg_audit_fga_policy_columns_detail;

-- enable_policy
select enable_policy(object_schema:='public', object_name:='foo', policy_name:='poli3');

select * from pg_audit_fga_conf;
select * from pg_audit_fga_conf_detail ;
select *from pg_audit_fga_policy_columns_detail;

-- remove invalid policy
\c audit_fga_database audit_fga_user
drop table if exists foo;

\c audit_fga_database audit_admin
select * from pg_audit_fga_conf;
select * from pg_audit_fga_conf_detail ;
select *from pg_audit_fga_policy_columns_detail;

select remove_valid_fga_policy();
select * from pg_audit_fga_conf;
select * from pg_audit_fga_conf_detail ;
select *from pg_audit_fga_policy_columns_detail;



