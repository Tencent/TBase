\set ECHO all

----prepare----
select current_database();
create user godlike superuser;
create user rubberneck superuser;
create user godlike2 superuser;
create user normal_user;
\c regression mls_admin
create user mls_normal_user createrole;
\c regression godlike

create extension tbase_mls;
select current_database();
select current_user;

-----------------USER ACL TEST BEGIN--------------------
\c regression godlike
create user mls_godlike;
alter user rubberneck rename to mls_rubberneck;

\c regression mls_admin
create user normal_user;
alter user mls_normal_user rename to mls_normal_user_tmp;
alter user mls_normal_user_tmp rename to mls_normal_user;
alter user normal_user rename to mls_normal_user_test;

drop user normal_user;
create user mls_drop_test;
drop user mls_drop_test;

\c regression godlike
drop user mls_normal_user;

\c regression godlike
create table security_tb (test_a int2, test_b int4, test_c int8, test_d varchar, test_e text) distribute by shard(test_a);
create table test_table_mask_all(id int, f1 int2, f2 int4, f3 int8, f4 varchar, f5 text, f6 char(20), f7 varchar2, f8 numeric, f9 timestamp, f10 float4, f11 float8);

\c regression mls_admin
select MLS_ACL_GRANT_USER_TABLE('public','security_tb','mls_normal_user');
select MLS_ACL_GRANT_USER_TABLE('public','test_table_mask_all','mls_normal_user');

\c regression mls_normal_user
select MLS_TRANSPARENT_CRYPT_CREATE_ALGORITHM('AES128', '1949');

select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'security_tb', 1);  
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'security_tb');  

select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'security_tb', 'test_b', 1);
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'security_tb', 'test_b');

select MLS_DATAMASK_CREATE('public', 'test_table_mask_all', 'f1', 1);
select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'test_table_mask_all', 'f1', 666);
select MLS_DATAMASK_DROP_TABLE_POLICY('public', 'test_table_mask_all', 'f1') ; 

select MLS_DATAMASK_CREATE_USER_POLICY('public', 'test_table_mask_all', 'f1', 'godlike');
select MLS_DATAMASK_ENABLE_USER_POLICY('public', 'test_table_mask_all', 'f1','godlike');
select MLS_DATAMASK_DISABLE_USER_POLICY('public', 'test_table_mask_all', 'f1','godlike');
select MLS_DATAMASK_DROP_USER_POLICY('public', 'test_table_mask_all', 'f1', 'godlike');
select MLS_TRANSPARENT_CRYPT_DROP_ALGORITHM(1);

\c regression mls_admin
select MLS_ACL_REVOKE_USER_TABLE('public','security_tb','mls_normal_user');
select MLS_ACL_REVOKE_USER_TABLE('public','test_table_mask_all','mls_normal_user');

\c regression mls_normal_user
select MLS_TRANSPARENT_CRYPT_CREATE_ALGORITHM('AES128', '1949');

select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'security_tb', 1);  
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'security_tb');  

select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'security_tb', 'test_b', 1);
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'security_tb', 'test_b');

select MLS_DATAMASK_CREATE('public', 'test_table_mask_all', 'f1', 1);
select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'test_table_mask_all', 'f1', 666);
select MLS_DATAMASK_DROP_TABLE_POLICY('public', 'test_table_mask_all', 'f1') ; 

select MLS_DATAMASK_CREATE_USER_POLICY('public', 'test_table_mask_all', 'f1', 'godlike');
select MLS_DATAMASK_DISABLE_USER_POLICY('public', 'test_table_mask_all', 'f1','godlike');
select MLS_DATAMASK_ENABLE_USER_POLICY('public', 'test_table_mask_all', 'f1','godlike');
select MLS_DATAMASK_DROP_USER_POLICY('public', 'test_table_mask_all', 'f1', 'godlike');

select MLS_TRANSPARENT_CRYPT_DROP_ALGORITHM(1);

\c regression mls_admin
select MLS_ACL_GRANT_USER_SCHEMA('public','mls_normal_user');

\c regression mls_normal_user
select MLS_TRANSPARENT_CRYPT_CREATE_ALGORITHM('AES128', '1949');

select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'security_tb', 1);  
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'security_tb');  

select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'security_tb', 'test_b', 1);
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'security_tb', 'test_b');

select MLS_DATAMASK_CREATE('public', 'test_table_mask_all', 'f1', 1);
select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'test_table_mask_all', 'f1', 666);
select MLS_DATAMASK_DROP_TABLE_POLICY('public', 'test_table_mask_all', 'f1') ; 

select MLS_DATAMASK_CREATE_USER_POLICY('public', 'test_table_mask_all', 'f1', 'godlike');
select MLS_DATAMASK_DISABLE_USER_POLICY('public', 'test_table_mask_all', 'f1','godlike');
select MLS_DATAMASK_ENABLE_USER_POLICY('public', 'test_table_mask_all', 'f1','godlike');
select MLS_DATAMASK_DROP_USER_POLICY('public', 'test_table_mask_all', 'f1', 'godlike');

select MLS_TRANSPARENT_CRYPT_DROP_ALGORITHM(1);

\c regression mls_admin
select MLS_ACL_REVOKE_USER_SCHEMA('public','mls_normal_user');

\c regression mls_normal_user
select MLS_TRANSPARENT_CRYPT_CREATE_ALGORITHM('AES128', '1949');

select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'security_tb', 1);  
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'security_tb');  

select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'security_tb_col', 'test_b', 1);   
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'security_tb_col', 'test_b');     

select MLS_DATAMASK_CREATE('public', 'test_table_mask_all', 'f1', 1);
select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'test_table_mask_all', 'f1', 666);
select MLS_DATAMASK_DROP_TABLE_POLICY('public', 'test_table_mask_all', 'f1') ; 

select MLS_DATAMASK_CREATE_USER_POLICY('public', 'test_table_mask_all', 'f1', 'godlike');
select MLS_DATAMASK_DISABLE_USER_POLICY('public', 'test_table_mask_all', 'f1','godlike');
select MLS_DATAMASK_ENABLE_USER_POLICY('public', 'test_table_mask_all', 'f1','godlike');
select MLS_DATAMASK_DROP_USER_POLICY('public', 'test_table_mask_all', 'f1', 'godlike');

select MLS_TRANSPARENT_CRYPT_DROP_ALGORITHM(1);

-----------------DATAMASK TEST BEGIN--------------------
\c regression godlike
create table tbl_datamask_xx( i int, i_m int, ii int2, ii_m int2, j bigint, j_m bigint, x varchar, x_m varchar, y text, y_m text) distribute by shard(i);
insert into tbl_datamask_xx values(1024, 1024, 7788, 7788, 42949672960,42949672960, '112233201804035566', '112233201804035566', 'abcdefghijk', 'abcdefghijk');
insert into tbl_datamask_xx(i, x) values(1025, 'all is null');
insert into tbl_datamask_xx(i, x, x_m, y_m) values(1026, 'all is null', 'this is a very very very looooooooong string, i guess here is over 32 bytes length, emmmmm', 'tbase!!~~~');


create table tbl_datamask_yy( i int, i_m int, ii int2, ii_m int2, j bigint, j_m bigint, x varchar, x_m varchar, y text, y_m text) distribute by shard(i);;
insert into tbl_datamask_yy values(1024, 1024, 7788, 7788, 42949672960,42949672960, '112233201804035566', '112233201804035566', 'abcdefghijk', 'abcdefghijk');

create table tbl_datamask_zz ( i int , j text , z text ) distribute by shard(i);;
insert into tbl_datamask_zz values(1024,'x','y');

--let mls_admin create datamask policy
\c regression mls_admin
select current_database();
select current_user;

--case1: api robustness test
--fail:optionval 
select MLS_DATAMASK_CREATE('public', 'tbl_datamask_xx', 'ii_m', 0);
--fail:schema_name 
select MLS_DATAMASK_CREATE('', 'tbl_datamask_xx', 'ii_m', 1);
select MLS_DATAMASK_CREATE('tbl_datamask_xx', 'ii_m', 1);
--fail:table_name 
select MLS_DATAMASK_CREATE('public', 'tbl_datamask_xxx', 'ii_m', 1);
--fail:attname 
select MLS_DATAMASK_CREATE('public', 'tbl_datamask_xx', 'xxx', 1);
--fail:over length
select MLS_DATAMASK_CREATE('public', 'tbl_datamask_xx', 'x_m',  2, 33);
--fail:system table is unsupported
select MLS_DATAMASK_CREATE('pg_catalog', 'pg_database', 'datdba',  1, 1024);
select MLS_DATAMASK_CREATE('pg_catalog', 'pg_database', 'datcollate',  2, 3);
select MLS_DATAMASK_CREATE('pg_catalog', 'pg_database', 'datcollate',  4, 3);
select MLS_DATAMASK_CREATE('pg_catalog', 'pg_database', 'datconnlimit',  3, 3);

--ok:indentify default value
select MLS_DATAMASK_CREATE('public', 'tbl_datamask_xx', 'i_m',  1);
select MLS_DATAMASK_CREATE('public', 'tbl_datamask_xx', 'ii_m', 1, 88888888);
select MLS_DATAMASK_CREATE('public', 'tbl_datamask_xx', 'j_m',  1, 9999999);
select MLS_DATAMASK_CREATE('public', 'tbl_datamask_xx', 'x_m',  2, 32);
select MLS_DATAMASK_CREATE('public', 'tbl_datamask_xx', 'y_m',  4);
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'tbl_datamask_xx', 'i_m',  'godlike');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'tbl_datamask_xx', 'ii_m', 'godlike');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'tbl_datamask_xx', 'j_m',  'godlike');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'tbl_datamask_xx', 'x_m',  'godlike');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'tbl_datamask_xx', 'y_m',  'godlike');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'tbl_datamask_xx', 'godlike2');

--case2:compare result among crowds
\c regression rubberneck
select * from tbl_datamask_xx order by i;
\c regression godlike
select * from tbl_datamask_xx order by i;
\c regression godlike2
select * from tbl_datamask_xx order by i;
--case3:update datamask datum value
\c regression mls_admin
select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'tbl_datamask_xx', 'i_m', 7777);
select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'tbl_datamask_xx', 'x_m', 5);
\c regression rubberneck
select * from tbl_datamask_xx order by i;

--case4:enable or disable datamask policy on one column
\c regression mls_admin
select MLS_DATAMASK_DISABLE_USER_POLICY('public', 'tbl_datamask_xx', 'i_m', 'godlike');
\c regression godlike
select * from tbl_datamask_xx order by i;
\c regression mls_admin
select MLS_DATAMASK_ENABLE_USER_POLICY('public', 'tbl_datamask_xx', 'i_m', 'godlike');
\c regression godlike
select * from tbl_datamask_xx order by i;

----the follows should fail, cause datamask column exist in expr
--case5:fail, check insert returning value
\c regression rubberneck
insert into tbl_datamask_xx (i, i_m)values(10240, 10240) returning i_m;
--case6:fail, join test, there should be no record returning, cause, i_m in xx has mask
select tbl_datamask_xx.i from tbl_datamask_xx, tbl_datamask_yy where tbl_datamask_xx.i_m = tbl_datamask_yy.i_m;
--case7:fail, select test, 
select * from tbl_datamask_xx where i_m = 1024 order by i;
select * from tbl_datamask_xx where i_m = 7777 order by i;
--case8:fail, join test, 
select * from tbl_datamask_xx xx, (select yy.i from tbl_datamask_yy yy, tbl_datamask_zz zz where yy.i=zz.i) a where xx.i = a.i order by xx.i;
select * from tbl_datamask_yy yy join tbl_datamask_xx xx on yy.i_m = xx.i_m order by xx.i;
select * from tbl_datamask_yy yy join tbl_datamask_xx xx using (i_m) order by xx.i;
select * from tbl_datamask_yy yy, (select z.i from tbl_datamask_zz z , ( select i, i_m from tbl_datamask_xx )x where x.i_m = 1024 and z.i = x.i) xx where yy.i = xx.i order by xx.i;

--case9:drop datamask policy
\c regression mls_admin
select MLS_DATAMASK_DROP_USER_POLICY('public', 'tbl_datamask_xx', 'i_m', 'godlike');
select MLS_DATAMASK_DROP_USER_POLICY('public', 'tbl_datamask_xx', 'x_m', 'godlike');
\c regression godlike
select * from tbl_datamask_xx order by i;


--case12:
\c regression godlike
create table tbl_xx(id int, tt timestamp, ff4 float4, ff8 float8, nn numeric, cc char, cc_2 char(1), cc_3 char(6), cc_4 char(10), varch2 varchar2);
insert into tbl_xx values(1024, '2018-09-10 1:2:3', 99.8, 777.777, 399.01, 'x', 'y', 'uiopw','abcefghijk', 'a long story');
select * from tbl_xx order by id;
\c regression mls_admin

--fail, timestamp type needs explicit mask value input
select MLS_DATAMASK_CREATE('public', 'tbl_xx', 'tt',     3);
select MLS_DATAMASK_CREATE('public', 'tbl_xx', 'tt',     3, '2099-10-10 10:10:10');
select MLS_DATAMASK_CREATE('public', 'tbl_xx', 'ff4',    3, 12345.6);
select MLS_DATAMASK_CREATE('public', 'tbl_xx', 'ff8',    3, 12345.678);
select MLS_DATAMASK_CREATE('public', 'tbl_xx', 'nn',     3, 500.001);
select MLS_DATAMASK_CREATE('public', 'tbl_xx', 'cc',     2, 12);
select MLS_DATAMASK_CREATE('public', 'tbl_xx', 'cc_2',   4, 12);
select MLS_DATAMASK_CREATE('public', 'tbl_xx', 'cc_3',   3, 'tbase security');
select MLS_DATAMASK_CREATE('public', 'tbl_xx', 'varch2', 4, 10);

select MLS_DATAMASK_CREATE_USER_POLICY('public', 'tbl_xx', 'tt',     'godlike');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'tbl_xx', 'ff4',    'godlike');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'tbl_xx', 'ff8',    'godlike');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'tbl_xx', 'nn',     'godlike');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'tbl_xx', 'cc',     'godlike');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'tbl_xx', 'cc_2',   'godlike');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'tbl_xx', 'cc_3',   'godlike');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'tbl_xx', 'varch2', 'godlike');

\c regression rubberneck
select * from tbl_xx;
\c regression godlike
select * from tbl_xx order by id;

--case13:
\c regression rubberneck
create table tbl_yy(id int, tt timestamp, ff4 float4, ff8 float8, nn numeric, cc char, cc_2 char(1), cc_3 char(6), cc_4 char(10), varch2 varchar2);
insert into tbl_yy select * from tbl_xx order by id asc;
select * from tbl_yy;
drop table tbl_yy;

--case, shard and partition table
\c regression godlike
create table tbl_datamask_shard_part(f1 int, f2 timestamp default now(), f3 int) 
partition by range (f2) 
    begin (timestamp without time zone '2018-01-01 0:0:0') step (interval '1 month') 
    partitions (6) 
    distribute by shard(f1) 
to group default_group ; 

insert into tbl_datamask_shard_part values( 1024, '2018-01-01 1:1:1', 1024);
insert into tbl_datamask_shard_part values( 2048, '2018-01-02 2:2:2', 2048);
insert into tbl_datamask_shard_part values( 3096, '2018-01-03 3:3:3', 3096);
insert into tbl_datamask_shard_part values( 4192, '2018-01-04 4:4:4', 4192);
insert into tbl_datamask_shard_part values( 7788, '2018-02-04 4:4:4', 4192);
insert into tbl_datamask_shard_part values( 7789, '2018-02-04 4:4:4', 4192);

select * from tbl_datamask_shard_part order by f1;

\c regression mls_admin
select MLS_DATAMASK_CREATE('public', 'tbl_datamask_shard_part', 'f1',  1, 9999);
select MLS_DATAMASK_CREATE('public', 'tbl_datamask_shard_part', 'f2',  3, '2099-09-09 9:9:9');
select MLS_DATAMASK_CREATE('public', 'tbl_datamask_shard_part', 'f3',  1, 10007);

\c regression godlike
select * from tbl_datamask_shard_part order by f1;


\c - godlike
create schema mls_schema;

create table mls_schema.test2(f1 int, f2 timestamp default now(), f3 int) 
to group default_group ;

create table mls_schema.test3_not_mls_policy(f1 int, f2 timestamp default now(), f3 int) 
to group default_group ;

insert into mls_schema.test2 values( 1024, '2018-01-01 1:1:1', 1024);
insert into mls_schema.test2 values( 2048, '2018-01-02 2:2:2', 2048);
insert into mls_schema.test2 values( 3096, '2018-01-03 3:3:3', 3096);
insert into mls_schema.test2 values( 4192, '2018-01-04 4:4:4', 4192);
select * from mls_schema.test2 order by f1;

insert into mls_schema.test3_not_mls_policy values( 1024, '2018-01-01 1:1:1', 1024);
insert into mls_schema.test3_not_mls_policy values( 2048, '2018-01-02 2:2:2', 2048);
insert into mls_schema.test3_not_mls_policy values( 3096, '2018-01-03 3:3:3', 3096);
insert into mls_schema.test3_not_mls_policy values( 4192, '2018-01-04 4:4:4', 4192);
select * from mls_schema.test3_not_mls_policy order by f1;

grant usage on schema mls_schema to mls_admin;   

\c regression mls_admin
select MLS_DATAMASK_CREATE('mls_schema', 'test2', 'f1',  1, 9999);
select MLS_DATAMASK_CREATE('mls_schema', 'test2', 'f2',  3, '2099-09-09 9:9:9');
select MLS_DATAMASK_CREATE('mls_schema', 'test2', 'f3',  1, 10007);

\c regression godlike
select * from mls_schema.test2 order by f1;

create user user_in_white_list superuser;
\c - user_in_white_list
select * from mls_schema.test2 order by f1;

\c - mls_admin
select MLS_DATAMASK_CREATE_USER_POLICY('mls_schema', 'test2', 'f1',     'user_in_white_list');
select MLS_DATAMASK_CREATE_USER_POLICY('mls_schema', 'test2', 'f2',    'user_in_white_list');
select MLS_DATAMASK_CREATE_USER_POLICY('mls_schema', 'test2', 'f3',    'user_in_white_list');
\c - user_in_white_list
select * from mls_schema.test2 order by f1;


\c regression godlike
create table tbl_datamask_unsupport(i int, bb point);
\c regression mls_admin
select MLS_DATAMASK_CREATE('public', 'tbl_datamask_unsupport', 'bb', 1);

--case, should fail, drop the user who is in datamask white list
\c - godlike
drop user user_in_white_list;
drop table tbl_datamask_unsupport;
--case, should fail, rename user who is in datamask white list
alter user user_in_white_list rename to user_in_white_list_2;

--case, should fail, alter column has mls policy
alter table mls_schema.test2 rename f2 to f2222;

--case, should fail, drop column has mls policy
alter table mls_schema.test2 drop column f2;

--case, should fail, drop table has mls policy
drop table mls_schema.test2, mls_schema.test3_not_mls_policy;
--while, mls_schema.test3_not_mls_policy has not been dropped.
select * from mls_schema.test2 order by f1;
select * from mls_schema.test3_not_mls_policy order by f1;

--case, should fail, rename table has mls policy
alter table mls_schema.test2 rename to test2222;

--case, should fail, drop schema has mls policy on table
--drop schema mls_schema;

--case, should fail, rename schema has mls policy on table
alter schema mls_schema rename to mls_schema2222;

--case, success, datamask could be dropped even table is not empty.
\c - mls_admin
select MLS_DATAMASK_DROP_TABLE_POLICY('mls_schema', 'test2', 'f1');

--fail, not empty
\c - godlike 
drop table mls_schema.test2;
truncate table mls_schema.test2;

--success
\c - mls_admin
select MLS_DATAMASK_DROP_TABLE_POLICY('mls_schema', 'test2', 'f1');
select MLS_DATAMASK_DROP_TABLE_POLICY('mls_schema', 'test2');

--success, empty and no policy
\c - godlike
drop table mls_schema.test2;
drop table mls_schema.test3_not_mls_policy;

--fail, cause table is dropped
\c - mls_admin
SELECT MLS_DATAMASK_DROP_USER_POLICY('mls_schema', 'test2', 'f1', 'user_in_white_list');
SELECT MLS_DATAMASK_DROP_USER_POLICY('mls_schema', 'test2', 'f1', 'user_in_white_list');
SELECT MLS_DATAMASK_DROP_USER_POLICY('mls_schema', 'test2', 'f1', 'user_in_white_list');

--success, not policy
--\c - godlike 
--drop user user_in_white_list;


---select with index
\c - godlike
drop table if exists data_mask_primary_key;
create table data_mask_primary_key(id int2 primary key, f1 int2, f2 int4, f3 int8);

insert into data_mask_primary_key(id,f1,f2,f3) values(1, 111, 222, 333);
insert into data_mask_primary_key(id,f1,f2,f3) values(2, 11, 22, 33);
insert into data_mask_primary_key(id,f1,f2,f3) values(3, 1, 2, 3);

\c - mls_admin
select MLS_DATAMASK_CREATE('public', 'data_mask_primary_key', 'id', 1);
select MLS_DATAMASK_CREATE('public', 'data_mask_primary_key', 'f1', 1);
select MLS_DATAMASK_CREATE('public', 'data_mask_primary_key', 'f2', 1);
select MLS_DATAMASK_CREATE('public', 'data_mask_primary_key', 'f3', 1);

\c - godlike
select * from data_mask_primary_key order by id asc;
truncate data_mask_primary_key;

\c - mls_admin
select MLS_DATAMASK_DROP_TABLE_POLICY('public', 'data_mask_primary_key');

--case14: varchar\text has default mask value
\c - godlike
drop table if exists test_table_e;
create table test_table_e(e_a int2, e_b int4, e_c int8, e_d varchar, e_e text, e_f char(20), e_g varchar2, e_h numeric, e_i timestamp, e_j float4, e_k float8) distribute by shard(e_a);

\c - mls_admin
select MLS_DATAMASK_CREATE('public', 'test_table_e', 'e_b', 1);
select MLS_DATAMASK_CREATE('public', 'test_table_e', 'e_c', 1);
select MLS_DATAMASK_CREATE('public', 'test_table_e', 'e_d', 3, 'ddd');
select MLS_DATAMASK_CREATE('public', 'test_table_e', 'e_e', 3, 'eee');
select MLS_DATAMASK_CREATE('public', 'test_table_e', 'e_f', 3, 'fff');
select MLS_DATAMASK_CREATE('public', 'test_table_e', 'e_g', 3, 'ggg');
select MLS_DATAMASK_CREATE('public', 'test_table_e', 'e_h', 3, 'hhh');
select MLS_DATAMASK_CREATE('public', 'test_table_e', 'e_i', 3, 'iii');
select MLS_DATAMASK_CREATE('public', 'test_table_e', 'e_j', 3, 'jjj');
select MLS_DATAMASK_CREATE('public', 'test_table_e', 'e_k', 3, 'kkk');

\c - godlike
insert into test_table_e(e_a, e_b, e_c, e_d, e_e, e_f, e_g, e_h, e_i, e_j, e_k) values(111, 222, 333, 'Tbase', 'Tbase', 'Tbase', 'Tbase', '123.123', '2018-08-08 8:8:8', '444.444', ' 888.888');
insert into test_table_e(e_a, e_b, e_c, e_d, e_e, e_f, e_g, e_h, e_i, e_j, e_k) values(444, 555, 666, 'Tbase', 'Tbase ', 'Tbase ', 'Tbase ', '1234.1234', '2019-09-09 9:9:9', '444555.444555', ' 888999.888999');
insert into test_table_e(e_a, e_b, e_c, e_d, e_e, e_f, e_g, e_h, e_i, e_j, e_k) values(777, 888, 999, 'hello world', 'hello tbase', 'hello 700', 'hello 007', '12345.12345', '2020-10-10 10:10:10', '44456.44456', ' 8889990.888999');
select * from test_table_e order by e_a;

\c - mls_admin
select MLS_DATAMASK_DROP_TABLE_POLICY('public', 'test_table_e');

\c - godlike
drop table test_table_e;

--case15: forbid detaching partition if mls policy exists
\c - godlike
drop table if exists alter_order_range;
create table alter_order_range(f1 int, f2 int2, f3 int4, f4 int8, f5 varchar, f6 text, f7 char(20), f8 varchar2, f9 numeric, f10 timestamp, f11 float4, f12 float8) partition by range (f10);

create table alter_order_range_201701 partition of alter_order_range(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12) for values from ('2017-01-01') to ('2017-02-01');
create table alter_order_range_201702 partition of alter_order_range(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12) for values from ('2017-02-01') to ('2017-03-01');

insert into alter_order_range(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12) values(1,111, 222, 333, 'Tbase', 'Tbase ', 'helloworld', 'Tbase', '123.123', '2017-01-02 0:0:0', '444.444', ' 888.888');
insert into alter_order_range(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12) values(2,111, 222, 333, 'Tbase', 'Tbase ', 'helloworld', 'Tbase', '123.123', '2017-02-02 0:0:0', '444.444', ' 888.888');

\c - mls_admin
select MLS_DATAMASK_CREATE('public', 'alter_order_range', 'f2', 1);
select MLS_DATAMASK_CREATE('public', 'alter_order_range', 'f3', 1);
select MLS_DATAMASK_CREATE('public', 'alter_order_range', 'f4', 1);
select MLS_DATAMASK_CREATE('public', 'alter_order_range', 'f5', 2);
select MLS_DATAMASK_CREATE('public', 'alter_order_range', 'f6', 2);
select MLS_DATAMASK_CREATE('public', 'alter_order_range', 'f7', 2);
select MLS_DATAMASK_CREATE('public', 'alter_order_range', 'f8', 2);
select MLS_DATAMASK_CREATE('public', 'alter_order_range', 'f9', 3);
select MLS_DATAMASK_CREATE('public', 'alter_order_range', 'f10', 3, '2015-05-05 5:5:5');
select MLS_DATAMASK_CREATE('public', 'alter_order_range', 'f11', 3);
select MLS_DATAMASK_CREATE('public', 'alter_order_range', 'f12', 3);

\c - godlike
select * from alter_order_range order by f1 asc;
select * from alter_order_range_201701 order by f1 asc;
select * from alter_order_range_201702 order by f1 asc;

sample alter_order_range(3000);

alter table alter_order_range detach partition alter_order_range_201701;

\c - mls_admin
select MLS_DATAMASK_DROP_TABLE_POLICY('public', 'alter_order_range');
--fails
\c - godlike
drop table alter_order_range;
\c - mls_admin
select MLS_DATAMASK_DROP_TABLE_POLICY('public', 'alter_order_range_201701');
select MLS_DATAMASK_DROP_TABLE_POLICY('public', 'alter_order_range_201702');
--success
\c - godlike 
drop table alter_order_range;
--case16: drop all column policies of the 'white list user' in batch
\c - mls_admin
select attnum,enable,username,nspname,tblname from pg_data_mask_user order by tblname, username, attnum;

\c - godlike
create table tbl_drop_white_list_by_col(f1 int, f2 int2, f3 int4, f4 int8) distribute by shard (f1);
create table tbl_drop_white_list_batch(f1 int, f2 int2, f3 int4, f4 int8) distribute by shard (f1);

\c - mls_admin
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'tbl_drop_white_list_by_col', 'f2', 'godlike');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'tbl_drop_white_list_by_col', 'f3', 'godlike');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'tbl_drop_white_list_by_col', 'f4', 'godlike');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'tbl_drop_white_list_batch',  'godlike');
select attnum,enable,username,nspname,tblname from pg_data_mask_user order by tblname, username, attnum;
select MLS_DATAMASK_DROP_USER_POLICY('public', 'tbl_drop_white_list_by_col',  'godlike');
select MLS_DATAMASK_DROP_USER_POLICY('public', 'tbl_drop_white_list_batch',  'godlike');
select attnum,enable,username,nspname,tblname from pg_data_mask_user order by tblname, username, attnum;

\c - godlike
drop table tbl_drop_white_list_by_col;
drop table tbl_drop_white_list_batch;

--case: orignal partitions would bind datamask to its children all, cover L1\L2\Lx....
drop table if exists order_range_list;
create table order_range_list(f1 int, f2 int2, f3 int4, f4 int8, f5 varchar, f6 text, f7 char(20), f8 varchar2, f9 numeric, f10 timestamp, f11 float4, f12 float8) partition by list (f6);

create table order_range_list_gd partition of order_range_list for values in ('gd') partition by range (f10);
create table order_range_list_bj partition of order_range_list for values in ('bj') partition by range (f10);
create table order_range_list_sh partition of order_range_list for values in ('sh');
create table order_range_list_gd_201701 partition of order_range_list_gd(f1 primary key,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12) for values from ('2017-01-01') to ('2017-02-01');
create table order_range_list_gd_201702 partition of order_range_list_gd(f1 primary key,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12) for values from ('2017-02-01') to ('2017-03-01');
create table order_range_list_gd_201703 partition of order_range_list_gd(f1 primary key,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12) for values from ('2017-03-01') to ('2017-04-01');
create table order_range_list_bj_201701 partition of order_range_list_bj(f1 primary key,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12) for values from ('2017-01-01') to ('2017-02-01');
create table order_range_list_bj_201702 partition of order_range_list_bj(f1 primary key,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12) for values from ('2017-02-01') to ('2017-03-01');
create table order_range_list_bj_201703 partition of order_range_list_bj(f1 primary key,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12) for values from ('2017-03-01') to ('2017-04-01');
insert into order_range_list(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12) values(1,111, 222, 333, 'Tbase', 'gd', 'hello tbase ', 'Tbase', '123.123', '2017-01-02 0:0:0', '444.444', ' 888.888');
insert into order_range_list(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12) values(2,111, 222, 333, 'Tbase', 'gd', 'hello tbase ', 'Tbase', '123.123', '2017-02-02 0:0:0', '444.444', ' 888.888');
insert into order_range_list(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12) values(3,111, 222, 333, 'Tbase', 'gd', 'hello tbase ', 'Tbase', '123.123', '2017-03-02 0:0:0', '444.444', ' 888.888');
insert into order_range_list(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12) values(4,111, 222, 333, 'Tbase', 'bj', 'hello beijing', 'Tbase', '123.123', '2017-01-02 0:0:0', '444.444', ' 888.888');
insert into order_range_list(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12) values(5,111, 222, 333, 'Tbase', 'bj', 'hello beijing', 'Tbase', '123.123', '2017-02-02 0:0:0', '444.444', ' 888.888');
insert into order_range_list(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12) values(6,111, 222, 333, 'Tbase', 'bj', 'hello beijing', 'Tbase', '123.123', '2017-03-02 0:0:0', '444.444', ' 888.888');
insert into order_range_list(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12) values(7,111, 222, 333, 'Tbase', 'sh', 'hello tailand', 'Tbase', '123.123', '2017-02-02 0:0:0', '444.444', ' 888.888');
insert into order_range_list(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12) values(8,111, 222, 333, 'Tbase', 'sh', 'hello tailand', 'Tbase', '123.123', '2017-01-02 0:0:0', '444.444', ' 888.888');

\c - mls_admin
select MLS_DATAMASK_CREATE('public', 'order_range_list', 'f2', 1);
select MLS_DATAMASK_CREATE('public', 'order_range_list', 'f3', 1);
select MLS_DATAMASK_CREATE('public', 'order_range_list', 'f4', 1);
select MLS_DATAMASK_CREATE('public', 'order_range_list', 'f5', 2);
select MLS_DATAMASK_CREATE('public', 'order_range_list', 'f6', 2);
select MLS_DATAMASK_CREATE('public', 'order_range_list', 'f7', 2);
select MLS_DATAMASK_CREATE('public', 'order_range_list', 'f8', 2);
select MLS_DATAMASK_CREATE('public', 'order_range_list', 'f9', 3);
select MLS_DATAMASK_CREATE('public', 'order_range_list', 'f10', 3, '2015-05-05 5:5:5');
select MLS_DATAMASK_CREATE('public', 'order_range_list', 'f11', 3);
select MLS_DATAMASK_CREATE('public', 'order_range_list', 'f12', 3);

select MLS_DATAMASK_CREATE_USER_POLICY('public', 'order_range_list', 'f2',  'godlike2');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'order_range_list', 'f3',  'godlike2');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'order_range_list', 'f4',  'godlike2');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'order_range_list', 'f5',  'godlike2');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'order_range_list', 'f6',  'godlike2');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'order_range_list', 'f7',  'godlike2');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'order_range_list', 'f8',  'godlike2');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'order_range_list', 'f9',  'godlike2');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'order_range_list', 'f10', 'godlike2');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'order_range_list', 'f11', 'godlike2');
select MLS_DATAMASK_CREATE_USER_POLICY('public', 'order_range_list', 'f12', 'godlike2');

--mask values
\c - godlike
select * from order_range_list order by f1 asc;
select * from order_range_list_gd order by f1 asc;
select * from order_range_list_bj order by f1 asc;
select * from order_range_list_sh order by f1 asc;
select * from order_range_list_gd_201701 order by f1 asc;
select * from order_range_list_gd_201702 order by f1 asc;
select * from order_range_list_gd_201703 order by f1 asc;
select * from order_range_list_bj_201701 order by f1 asc;
select * from order_range_list_bj_201702 order by f1 asc;
select * from order_range_list_bj_201703 order by f1 asc;

--orignal values
\c - godlike2
select * from order_range_list order by f1 asc;
select * from order_range_list_gd order by f1 asc;
select * from order_range_list_bj order by f1 asc;
select * from order_range_list_sh order by f1 asc;
select * from order_range_list_gd_201701 order by f1 asc;
select * from order_range_list_gd_201702 order by f1 asc;
select * from order_range_list_gd_201703 order by f1 asc;
select * from order_range_list_bj_201701 order by f1 asc;
select * from order_range_list_bj_201702 order by f1 asc;
select * from order_range_list_bj_201703 order by f1 asc;

--clean
\c - mls_admin
select MLS_DATAMASK_DROP_TABLE_POLICY('public', 'order_range_list');
select MLS_DATAMASK_DROP_TABLE_POLICY('public', 'order_range_list_bj');
select MLS_DATAMASK_DROP_TABLE_POLICY('public', 'order_range_list_sh');
select MLS_DATAMASK_DROP_TABLE_POLICY('public', 'order_range_list_gd');
--fail to clean all
select tblname, datamask, attnum from pg_data_mask_map where tblname ilike 'order_range_list%' order by 1, 2, 3;
--use casecade
select MLS_DATAMASK_DROP_TABLE_POLICY('public', 'order_range_list', true);
--there is no user policy too
select tblname , username, attnum from pg_data_mask_user where tblname ilike 'order_range_list%' order by 1, 2,3;
\c - godlike
drop table order_range_list;

---test for MLS_DATAMASK_UPDATE_TABLE_POLICY
\c - godlike 
create table test_table_mask_update(id int, f1 int2, f2 int4, f3 int8, f4 varchar, f5 text, f6 char(20), f7 varchar2, f8 numeric, f9 timestamp, f10 float4, f11 float8, f12 varchar, f13 text, f14 char(20), f15 varchar2);
insert into test_table_mask_update values(1, 2, 3,4, 'a','b','c','d',5,'2018-01-01',6,7,'e','f','g','h');
select * from test_table_mask_update;
\c - mls_admin
select MLS_DATAMASK_CREATE('public', 'test_table_mask_update', 'f1', 1);
select MLS_DATAMASK_CREATE('public', 'test_table_mask_update', 'f2', 1);
select MLS_DATAMASK_CREATE('public', 'test_table_mask_update', 'f3', 1);

select MLS_DATAMASK_CREATE('public', 'test_table_mask_update', 'f4', 2);
select MLS_DATAMASK_CREATE('public', 'test_table_mask_update', 'f5', 2);
select MLS_DATAMASK_CREATE('public', 'test_table_mask_update', 'f6', 2);
select MLS_DATAMASK_CREATE('public', 'test_table_mask_update', 'f7', 2);

select MLS_DATAMASK_CREATE('public', 'test_table_mask_update', 'f12', 4);
select MLS_DATAMASK_CREATE('public', 'test_table_mask_update', 'f13', 4);
select MLS_DATAMASK_CREATE('public', 'test_table_mask_update', 'f14', 4);
select MLS_DATAMASK_CREATE('public', 'test_table_mask_update', 'f15', 4);

select MLS_DATAMASK_CREATE('public', 'test_table_mask_update', 'f8', 3);
select MLS_DATAMASK_CREATE('public', 'test_table_mask_update', 'f9', 3, '2018-05-30 15:16:57');
select MLS_DATAMASK_CREATE('public', 'test_table_mask_update', 'f10', 3);
select MLS_DATAMASK_CREATE('public', 'test_table_mask_update', 'f11', 3);
\c - godlike
select * from test_table_mask_update;
\c - mls_admin
select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'test_table_mask_update', 'f1', 666);
select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'test_table_mask_update', 'f2', 777);
select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'test_table_mask_update', 'f3', 888);

select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'test_table_mask_update', 'f4', 6);
select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'test_table_mask_update', 'f5', 6);
select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'test_table_mask_update', 'f6', 6);
select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'test_table_mask_update', 'f7', 6);

select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'test_table_mask_update', 'f12', 5);
select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'test_table_mask_update', 'f13', 5);
select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'test_table_mask_update', 'f14', 5);
select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'test_table_mask_update', 'f15', 5);

select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'test_table_mask_update', 'f8', 3);
select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'test_table_mask_update', 'f9', '1970-01-01');
select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'test_table_mask_update', 'f10', 3);
select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'test_table_mask_update', 'f11', 3);
\c - godlike
select * from test_table_mask_update;
\c - mls_admin
select MLS_DATAMASK_DROP_TABLE_POLICY('public', 'test_table_mask_update');
\c - godlike
drop  table  test_table_mask_update;

--test for MLS_DATAMASK_UPDATE_TABLE_POLICY with orignal partition
\c - godlike
create table order_range_list(id bigserial not null,userid integer,product text,area text, createdate timestamp) partition by list ( area );
create table order_range_list_gd partition of order_range_list for values in ('guangdong') partition by range(createdate); 
create table order_range_list_gd_201701 partition of order_range_list_gd(id primary key,userid,product,area,createdate) for values from ('2017-01-01') to ('2017-02-01'); 

\c - mls_admin
select MLS_DATAMASK_CREATE('public', 'order_range_list', 'area', 2);
select MLS_DATAMASK_CREATE('public', 'order_range_list', 'createdate', 3, '1970-01-01');
select attnum, option, datamask, tblname, defaultval from pg_data_mask_map where tblname ilike 'order_range_list%' order by 1,2,3,4,5;

select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'order_range_list', 'area', 6);
select MLS_DATAMASK_UPDATE_TABLE_POLICY('public', 'order_range_list', 'createdate', '2999-9-9');
select attnum, option, datamask, tblname, defaultval from pg_data_mask_map where tblname ilike 'order_range_list%' order by 1,2,3,4,5;

select MLS_DATAMASK_DROP_TABLE_POLICY('public', 'order_range_list', true);
select attnum, option, datamask, tblname, defaultval from pg_data_mask_map where tblname ilike 'order_range_list%' order by 1,2,3,4,5;

\c - godlike
drop table order_range_list;

--done all------------------------------------
\c regression godlike
drop table tbl_datamask_xx;
drop table tbl_datamask_yy;
drop table tbl_datamask_zz;
drop table tbl_xx;
drop table tbl_datamask_shard_part;
drop table data_mask_primary_key;
truncate tbl_xx;

\c - mls_admin
select MLS_DATAMASK_DROP_TABLE_POLICY('public', 'tbl_xx');
select MLS_DATAMASK_DROP_TABLE_POLICY('public', 'tbl_datamask_shard_part');
select MLS_DATAMASK_DROP_TABLE_POLICY('public', 'tbl_datamask_xx');
\c - godlike
drop table tbl_xx;
drop table tbl_datamask_xx;
drop table tbl_datamask_shard_part;

-----------------DATAMASK TEST END--------------------

-----------------TRANSPARENT CRYPT TEST BEGIN--------------------

--prepare
\c regression godlike
create table xx( i int, i_m int, ii int2, ii_m int2, j bigint, j_m bigint, x varchar, x_m varchar, y text, y_m text);

create table yy( i int, i_m int, ii int2, ii_m int2, j bigint, j_m bigint, x varchar, x_m varchar, y text, y_m text);
insert into yy values(1024, 1024, 7788, 7788, 42949672960,42949672960, '112233201804035566', '112233201804035566', 'abcdefghijk', 'abcdefghijk');

create table zz ( i int , j char , z char );
insert into zz values(1024,'x','y');

--create crypt algorithm
\c regression mls_admin
select MLS_TRANSPARENT_CRYPT_CREATE_ALGORITHM('AES128', '8888');
select MLS_TRANSPARENT_CRYPT_CREATE_ALGORITHM('AES192', '7777');
select MLS_TRANSPARENT_CRYPT_CREATE_ALGORITHM('AES256', '6666');
select MLS_TRANSPARENT_CRYPT_CREATE_ALGORITHM('SM4', '123456789abcdefg');


--binding crypt policy to table
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'xx', 'x_m', 1);
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'xx', 'y_m', 3);
--fail, dup bind
--select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'xx', 'x_m', 2);

--case1: check result, all in decrypt datum value
\c regression godlike
insert into xx values(1024, 1024, 7788, 7788, 42949672960,42949672960, '112233201804035566', '112233201804035566', 'abcdefghijk', 'abcdefghijk');
insert into xx(i, x) values(1025, 'all is null');
select * from xx order by i;

--fail, there are recode in table xx
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'xx', 'x_m');
--fail, if table is not empty, can not bind crypt algo
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'xx', 'j_m', 2);
--fail, system table is unsupported
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('pg_catalog', 'pg_database', 'datconnlimit', 1);
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('pg_catalog', 'pg_database', 1);

--case2: we arrange var-length and fix-length datatype mixed with each other
\c regression godlike
create table tbl_complex(i int, x text, j smallint, y varchar, k bigint, z text, o text, t timestamp, f4 float4, f8 float8, n1 numeric, var2 varchar2, c1 char, c2 char(3));
\c  regression mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'i');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'x');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'j');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'y');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'k');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'z');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'o');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 't');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'f4');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'f8');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'n1');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'var2');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'c1');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'c2');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_complex', 'i', 1);
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_complex', 'x', 2);
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_complex', 'j', 3);
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_complex', 'y', 1);
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_complex', 'k', 2);
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_complex', 'z', 3);
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_complex', 'o', 1);
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_complex', 't', 2);
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_complex', 'f4',3);
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_complex', 'f8',   1);
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_complex', 'n1',   2);
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_complex', 'var2', 3);
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_complex', 'c1', 1);
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_complex', 'c2', 2);

select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_complex', 1);

--check the result                                                               
\c regression godlike
insert into tbl_complex values(1024, 'xyz', 4499, 'abcdefg', 8877, 'this is blanc', 'rock and rooo~~', '2018-08-08 8:8:8', 111.11, 1234567890.123, 987654321.123, 'this is a varchar2 string', 'p', 'dp');
insert into tbl_complex(i) values(1025);
select * from tbl_complex order by i;
delete from tbl_complex;

--case3: fail: unsupported datatype to bind crypt policy, that shoule be failed
\c regression godlike
create table tbl_transparent_unsupport(i int, bb point);
\c regression mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_transparent_unsupport', 'bb', 1);
\c regression godlike
insert into tbl_transparent_unsupport values(1024, '(0.0,0.0)');
select * from tbl_transparent_unsupport order by i;
\c regression mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_transparent_unsupport', 'bb');

--case4: drop crypt policy on table, while, that should be forbidden, now returning messy code
--select TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'x');
--select TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'y');
--select TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'z');
--select TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'k');
--\c regression godlike
--select * from tbl_complex;

--case, algo can not be drop if it was used
select MLS_TRANSPARENT_CRYPT_DROP_ALGORITHM(1);

--case: interval partition with index
\c - godlike 
--create extension tbase_mls;
--create default node group default_group with(datanode_1, datanode_2); 
--create sharding group to group default_group;

create table tbl_mls_test(f1 int, f2 timestamp default now(), f3 int) 
partition by range (f2) 
    begin (timestamp without time zone '2018-01-01 0:0:0') step (interval '1 month') 
    partitions (3) 
    distribute by shard(f1) 
to group default_group ; 
alter table tbl_mls_test add partitions 1;

create table tbl_mls_test22(f1 int, f2 timestamp default now(), f3 int) 
partition by range (f2) 
    begin (timestamp without time zone '2018-01-01 0:0:0') step (interval '1 month') 
    partitions (3) 
    distribute by shard(f1) 
to group default_group ; 


\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_mls_test', 1);

\c - godlike 
insert into tbl_mls_test values(1024, '2018-01-01', 1111);
insert into tbl_mls_test values(1024, '2018-02-01', 1111);
insert into tbl_mls_test values(1024, '2018-03-01', 1111);
insert into tbl_mls_test values(1024, '2018-04-01', 1111);
create index idx_tbl_mls_test on tbl_mls_test(f1, f3);
alter table tbl_mls_test add partitions 1;
insert into tbl_mls_test select generate_series(1, 10000), '2018-05-01', generate_series(1, 10000);
insert into tbl_mls_test select generate_series(1, 10), '2018-06-01', generate_series(1, 10);


insert into tbl_mls_test22 values(1024, '2018-01-01', 1111);
insert into tbl_mls_test22 values(1024, '2018-02-01', 1111);
insert into tbl_mls_test22 values(1024, '2018-03-01', 1111);
insert into tbl_mls_test22 values(1024, '2018-04-01', 1111);
create index idx_tbl_mls_test22 on tbl_mls_test22(f1, f3);
alter table tbl_mls_test22 add partitions 2;
insert into tbl_mls_test22 select generate_series(1, 10000), '2018-05-01', generate_series(1, 10000);
insert into tbl_mls_test22 select generate_series(1, 10), '2018-06-01', generate_series(1, 10);
checkpoint;

--explain select * from tbl_mls_test where f1 = 1024 and f2 >= '2018-05-01' and f2 < '2018-06-01' order by f1 limit 10 ;      
select * from tbl_mls_test where f1 = 1024 and f2 >= '2018-05-01' and f2 < '2018-06-01' order by f1 limit 10 ;      

-- test vacuum analyze
vacuum analyze tbl_mls_test;

--case: orignal partition, interval partition with index
\c - godlike
create table tbl_mls_part_list( a int ,b int ) PARTITION BY LIST (b) ;
CREATE TABLE tbl_mls_part_list_1 PARTITION OF tbl_mls_part_list FOR VALUES IN (1);
CREATE TABLE tbl_mls_part_list_2 PARTITION OF tbl_mls_part_list FOR VALUES IN (2); 
CREATE TABLE tbl_mls_part_list_3 PARTITION OF tbl_mls_part_list FOR VALUES IN (3);
CREATE TABLE tbl_mls_part_list_4 PARTITION OF tbl_mls_part_list FOR VALUES IN (4); 

\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_mls_part_list',2); 

\c - godlike
insert into tbl_mls_part_list select   generate_series(1, 1000), 1;
create index idx_tbl_mls_part_list_1_a on tbl_mls_part_list_1 (a);
insert into tbl_mls_part_list select   generate_series(1, 1000), 1;
create index idx_tbl_mls_part_list_1_b on tbl_mls_part_list_1 (b);
checkpoint;
--explain select * from tbl_mls_part_list where a = 999 and b = 1 order by a limit 10 ;
select * from tbl_mls_part_list where a = 999 and b = 1  order by a limit 10 ;

\c - godlike
create table tbl_complex_guomin_333(i int, x text, j smallint, y varchar, k bigint, z text, o text, t timestamp, f4 float4, f8 float8, n1 numeric, var2 varchar2, c1 char, c2 char(3)) distribute by shard(i);
\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_complex_guomin_333', 4); 
\c - godlike
insert into tbl_complex_guomin_333 select generate_series(1, 10000), 'xyz', 4499, 'abcdefg', 8877, 'this is blancthis is blancthis is blancthis is blancthis is blancthis is blancthis is blancthis is blancthis is blancthis is blancthis is blancthis is blancthis is blancthis is blancthis is blanc', 'rock and rooo~~rock and rooo~~rock and rooo~~rock and rooo~~rock and rooo~~rock and rooo~~rock and rooo~~rock and rooo~~rock and rooo~~rock and rooo~~rock and rooo~~rock and rooo~~ and rooo~~rock and rooo~~', '2018-08-08 8:8:8', 111.11, 1234567890.123, 987654321.123, 'this is a varchar2 string', 'p', 'dp';
checkpoint;
select * from tbl_complex_guomin_333 where i > 9990 order by i;
truncate tbl_complex_guomin_333;

--case: schema crypt
\c - godlike
create schema crypt_schema_5;
\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_SCHEMA('crypt_schema_5', 4);
\c - godlike
create table crypt_schema_5.tbl_crypt_5(id int, number int) distribute by shard(id);
create index idx_tbl_crypt_5_id on crypt_schema_5.tbl_crypt_5(id);
insert into crypt_schema_5.tbl_crypt_5 select generate_series(1, 100), 1024;
create table crypt_schema_5.tbl_crypt_6(id int, number int) distribute by shard(id);
create index idx_tbl_crypt_6_id on crypt_schema_5.tbl_crypt_6(id);
insert into crypt_schema_5.tbl_crypt_6 select generate_series(1, 100), 1024;
checkpoint;
select * from crypt_schema_5.tbl_crypt_5 where id < 5 order by id;
truncate crypt_schema_5.tbl_crypt_5;
truncate crypt_schema_5.tbl_crypt_6;

--ok, table in crypted schema could be moved to other place.
alter table crypt_schema_5.tbl_crypt_6 set schema public;

\c - godlike 
--sucess, index could be drop directly.
drop index public.idx_tbl_crypt_6_id;
--success, even mls_admin dose not unbind tbl_crypt_5
drop table crypt_schema_5.tbl_crypt_5;

\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_crypt_6'); 
\c - godlike
--sucess, after unbound
drop table public.tbl_crypt_6;
\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_SCHEMA('crypt_schema_5');
\c - godlike
drop schema crypt_schema_5;

\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex_guomin_333'); 
--select MLS_TRANSPARENT_CRYPT_DROP_ALGORITHM(4);

--case set, for schema test
\c - godlike 
create schema crypt_schema_sm4;
create table crypt_schema_sm4.tbl_yy(i int, j int) distribute by shard(i);
create table tbl_zz ( i int, j int) distribute by shard(i);
\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_SCHEMA('crypt_schema_sm4', 4);
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname in( 'tbl_zz' ,'tbl_yy') order by tblname;
\c - godlike
--ok
--to test policy record deleted cascaded when table dropped
create table crypt_schema_sm4.tbl_mls_part_list( a int ,b int ) PARTITION BY LIST (b) ;
CREATE TABLE crypt_schema_sm4.tbl_mls_part_list_1 PARTITION OF crypt_schema_sm4.tbl_mls_part_list FOR VALUES IN (1);
create table crypt_schema_sm4.tbl_mls_test(f1 int, f2 timestamp default now(), f3 int) 
partition by range (f2) 
    begin (timestamp without time zone '2018-01-01 0:0:0') step (interval '1 month') 
    partitions (3) 
    distribute by shard(f1) 
to group default_group ; 
alter table crypt_schema_sm4.tbl_mls_test add partitions 1;
--ok, crypt_schema_sm4.tbl_yy is not crypted
alter table crypt_schema_sm4.tbl_yy set schema public ;
--ok, tbl_zz is not crypted
alter table tbl_zz set schema crypt_schema_sm4;
--ok
create table crypt_schema_sm4.tbl_nn(i int, j int) distribute by shard(i);
\c - mls_admin
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname = 'tbl_nn';
\c - godlike
--ok, crypt_schema_sm4 and crypt_schema_sm4.tbl_nn has crypted
alter table crypt_schema_sm4.tbl_nn set schema public;
\c - mls_admin
select nspname, tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map order by 1,2,3;
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_nn'); 
--ok 
\c - godlike
drop table public.tbl_yy, public.tbl_nn, crypt_schema_sm4.tbl_zz, crypt_schema_sm4.tbl_mls_test, crypt_schema_sm4.tbl_mls_part_list, crypt_schema_sm4.tbl_mls_part_list_1;
\c - mls_admin
--policies record should be deleted
select nspname, tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map  order by 1,2,3;
\c - godlike
--fail, crypt_schema_sm4 is not empty
alter schema crypt_schema_sm4 rename to crypt_schema_sm5;
drop schema crypt_schema_sm4;
--unbind schema
\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_SCHEMA('crypt_schema_sm4');
\c - godlike
alter schema crypt_schema_sm4 rename to crypt_schema_sm5;
drop schema crypt_schema_sm5;
create table tbl_col_sm4(normala int, normalb int, encrypted varchar) distribute by shard(normala);
\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_col_sm4', 'encrypted', 4);
\c - godlike
insert into tbl_col_sm4 values(1, 11, '1111dfa11');
insert into tbl_col_sm4 values(2, 22, repeat('a', 16));
insert into tbl_col_sm4 values(3, 33, 'dsfanle1={ntwkqweg-dibjf"sdfaw21(){{()"wjqtoij2j 199');
select * from tbl_col_sm4 order by 1;
truncate tbl_col_sm4;
\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_col_sm4', 'encrypted');
\c - godlike
drop table tbl_col_sm4;

--case rename tables in crypted schema
\c - godlike 
create schema crypt_schema_sm66;
\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_SCHEMA('crypt_schema_sm66', 4);
\c - godlike
create table crypt_schema_sm66.tbl_mls_part_list( a int ,b int ) PARTITION BY LIST (b) ;
CREATE TABLE crypt_schema_sm66.tbl_mls_part_list_1 PARTITION OF crypt_schema_sm66.tbl_mls_part_list FOR VALUES IN (1);
alter table crypt_schema_sm66.tbl_mls_part_list rename to tbl_mls_part_list_rename;
\c - mls_admin
--expect crypt_schema_sm66.tbl_mls_part_list was renamed.
select nspname, tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map  order by 1,2,3;
\c - godlike
create table crypt_schema_sm66.tbl_mls_test(f1 int, f2 timestamp default now(), f3 int) 
partition by range (f2) 
    begin (timestamp without time zone '2018-01-01 0:0:0') step (interval '1 month') 
    partitions (3) 
    distribute by shard(f1) 
to group default_group ; 
--fail: could not rename interval partition or its index
alter table crypt_schema_sm66.tbl_mls_test rename to tbl_mls_test_rename;
\c - godlike
select relname from pg_class c, pg_namespace n where c.relnamespace = n.oid and n.nspname = 'crypt_schema_sm66' order by 1;
drop table crypt_schema_sm66.tbl_mls_part_list_rename, crypt_schema_sm66.tbl_mls_part_list_1, crypt_schema_sm66.tbl_mls_test;
select relname from pg_class c, pg_namespace n where c.relnamespace = n.oid and n.nspname = 'crypt_schema_sm66' order by 1;
\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_SCHEMA('crypt_schema_sm66');
\c - godlike
drop schema crypt_schema_sm66;

--case set, for orignal partition tables
--case1, parent and child L1 was created, bind crypt policy to parent
\c - godlike
create table order_range_list(id bigserial not null,userid integer,product text,area text, createdate date) partition by list ( area );
create table order_range_list_gd partition of order_range_list for values in ('guangdong') partition by range(createdate); 
\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'order_range_list', 1); 

--1.1 both of them would be bound.
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'order_range_list%' order by tblname;

--1.2 while, create L2 child, L2 child dose not have policy.
\c - godlike 
create table order_range_list_gd_201701 partition of order_range_list_gd(id primary key,userid,product,area,createdate) for values from ('2017-01-01') to ('2017-02-01'); 
\c - mls_admin
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'order_range_list%' order by tblname;

--1.3 attach a noncrypt child, there would be not crypted bound to it.
\c - godlike
create table order_range_list_bj(id bigserial not null,userid integer,product text,area text, createdate date) partition by range(createdate); 
alter table order_range_list attach partition order_range_list_bj for values in ('beijing'); 
\c - mls_admin
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'order_range_list%' order by tblname;

--1.4 attach a crypted child, it would keep its own crypt policy.
\c - godlike
create table order_range_list_sh(id bigserial not null,userid integer,product text,area text, createdate date) partition by range(createdate); 
\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'order_range_list_sh', 2); 
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'order_range_list%' order by tblname;
\c - godlike
alter table order_range_list attach partition order_range_list_sh for values in ('shanghai'); 
\c - mls_admin
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'order_range_list%' order by tblname;

--1.5 detach the non crypt child, ok
\c - godlike
alter table order_range_list detach partition order_range_list_bj;

--1.6 detach the crypted child, while admin is forbidden to do this, mls_admin has the right.
\c - godlike
alter table order_range_list detach partition order_range_list_sh;
\c - mls_admin
alter table order_range_list detach partition order_range_list_sh;
--1.7 mls_admin could not detach uncrypt child, bj is noncrypt
\c - godlike
alter table order_range_list attach partition order_range_list_bj for values in ('beijing') ;
\c - mls_admin
alter table order_range_list detach partition order_range_list_bj;

--1.8 mls_admin could not attach child, no matter crypt or noncrypt
\c - mls_admin
alter table order_range_list attach partition order_range_list_bj for values in ('beijing') ;
alter table order_range_list attach partition order_range_list_sh for values in ('shanghai') ;

\c - godlike
alter table order_range_list attach partition order_range_list_sh for values in ('shanghai') ;

--1.9 L2 child has crypt bound, root parent would get error
\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_bj'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_sh'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_gd'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_gd_201701'); 
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'order_range_list%' order by tblname;
--bind L2
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'order_range_list_gd_201701', 1); 
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'order_range_list%' order by tblname;
--bind root parent error
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'order_range_list', 1); 
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'order_range_list%' order by tblname;
--clear
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list', true); 
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'order_range_list%' order by tblname;
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_gd_201701', true); 
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'order_range_list%' order by tblname;

--1.10 root parent bind crypt, its children would be bound too.
\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'order_range_list', 1); 
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'order_range_list%' order by tblname;
--clear
\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_bj'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_sh'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_gd'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_gd_201701'); 
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'order_range_list%' order by 1,2,3;

--1.11 root parent bind column crypt, its children would be bound too. And column is bound seperately.
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'order_range_list_gd_201701', 'userid', 1); 
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'order_range_list%' order by 1,2,3;
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'order_range_list_gd_201701', 'product', 1); 
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'order_range_list%' order by 1,2,3;
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_bj'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_sh'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_gd'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_gd_201701'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_gd_201701', 'product'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_gd_201701', 'userid'); 

--1.12 file crypt and column crypt can not affect with each other.
--1.12.1
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'order_range_list_gd_201701', 1); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'order_range_list_gd_201701', 'userid', 1); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'order_range_list_gd_201701', 'product', 1); 
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'order_range_list%' order by tblname;
--clean
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_gd_201701'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_gd_201701','userid'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_gd_201701','product'); 
--1.12.2
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'order_range_list_gd_201701', 'userid', 1); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'order_range_list_gd_201701', 1); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'order_range_list_gd_201701', 'product', 1); 
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'order_range_list%' order by tblname;
--clean
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_gd_201701'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_gd_201701','userid'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_gd_201701','product'); 

--clear case 1
\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_bj'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_sh'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_gd'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'order_range_list_gd_201701'); 
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'order_range_list%' order by tblname;
\c - godlike
drop table order_range_list;

--case2 parent and child have no crypt policy, bind crypt to the child, parent would not bound with crypt.
\c - godlike
CREATE TABLE tbl_tbase (
    f1 bigint,
    f2 text
)
PARTITION BY RANGE (f1)
DISTRIBUTE BY SHARD (f1) to GROUP default_group;
CREATE TABLE tbl_tbase_1 PARTITION OF tbl_tbase FOR VALUES FROM (1) TO (10000000);

\c - mls_admin 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_tbase_1', 1); 
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'tbl_tbase%' order by tblname;

--case2.1 bind parent crypt would fail
\c - mls_admin 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_tbase', 1); 

--case2.2 detach the child by mls_admin, bind parent crypt policy, then attach this crypt child 
\c - mls_admin 
alter table tbl_tbase detach partition tbl_tbase_1;
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_tbase', 1); 
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'tbl_tbase%' order by tblname;
\c - godlike
alter table tbl_tbase attach partition tbl_tbase_1 FOR VALUES FROM (1) TO (10000000);

--clean case2
\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_tbase'); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_tbase_1'); 
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'tbl_tbase%' order by tblname;
\c - godlike
drop table tbl_tbase;

--case3 parent has no crypt, but child has. drop table would fail
\c - godlike
CREATE TABLE tbl_tbase (
    f1 bigint,
    f2 text
)
PARTITION BY RANGE (f1)
DISTRIBUTE BY SHARD (f1) to GROUP default_group;
CREATE TABLE tbl_tbase_1 PARTITION OF tbl_tbase FOR VALUES FROM (1) TO (10000000);
\c - mls_admin 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_tbase_1', 1); 
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'tbl_tbase%' order by tblname;
\c - godlike
drop table tbl_tbase;

--case3.1 detach the crypt child, drop ok
\c - mls_admin 
alter table tbl_tbase detach partition tbl_tbase_1;
\c - godlike
drop table tbl_tbase;
--case3.2 unbind crypt, drop ok
\c - godlike
CREATE TABLE tbl_tbase (
    f1 bigint,
    f2 text
)
PARTITION BY RANGE (f1)
DISTRIBUTE BY SHARD (f1) to GROUP default_group;
alter table tbl_tbase attach partition tbl_tbase_1 FOR VALUES FROM (1) TO (10000000);
\c - mls_admin 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_tbase_1'); 
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'tbl_tbase%' order by tblname;
\c - godlike
drop table tbl_tbase;

--case4 unbind crypt would not be cascaded, parent is empty, crypt could be dropped
\c - godlike
CREATE TABLE tbl_tbase (
    f1 bigint,
    f2 text
)
PARTITION BY RANGE (f1)
DISTRIBUTE BY SHARD (f1) to GROUP default_group;
CREATE TABLE tbl_tbase_1 PARTITION OF tbl_tbase FOR VALUES FROM (1) TO (10000000);
\c - mls_admin 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('public', 'tbl_tbase', 1); 
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'tbl_tbase%' order by tblname;

--case4.1 crypt could be dropped on child if child is empty.
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_tbase'); 
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'tbl_tbase%' order by tblname;

select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_tbase_1'); 
select tblname, attnum, algorithm_id from pg_transparent_crypt_policy_map where tblname ilike 'tbl_tbase%' order by tblname;

--clean case4
\c - godlike
drop table tbl_tbase;

--case 5 materialized view
\c - godlike 
create schema crypt_schema_materializedview_single;
\c - mls_admin 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_SCHEMA('crypt_schema_materializedview_single', 4);
\c - godlike 
create table crypt_schema_materializedview_single.crypt_schema_materializedview_single(f1 int, f2 int2, f3 int4, f4 int8, f5 varchar, f6 text, f7 char(20), f8 varchar2, f9 numeric, f10 timestamp, f11 float4, f12 float8);
create materialized view crypt_schema_materializedview_single.crypt_schema_materializedview_single_v as select f1,f2,f5,f10 from crypt_schema_materializedview_single.crypt_schema_materializedview_single ;

\c - mls_admin
select attnum,algorithm_id,spcoid,spcname,nspname,tblname from pg_transparent_crypt_policy_map where tblname ilike 'crypt_schema_materializedview_single%';

\c - godlike
drop materialized view crypt_schema_materializedview_single.crypt_schema_materializedview_single_v ;
drop table crypt_schema_materializedview_single.crypt_schema_materializedview_single ;

--materialized view should be clean
\c - mls_admin
select attnum,algorithm_id,spcoid,spcname,nspname,tblname from pg_transparent_crypt_policy_map where tblname ilike 'crypt_schema_materializedview_single%';
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_SCHEMA('crypt_schema_materializedview_single');

--case 6 orignal partition delete cascaded
\c - godlike 
create schema crypt_schema_partition_range;
\c - mls_admin 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_SCHEMA('crypt_schema_partition_range', 4);
\c - godlike
create table crypt_schema_partition_range.crypt_schema_partition_range(f1 int, f2 int2, f3 int4, f4 int8, f5 varchar, f6 text, f7 char(20), f8 varchar2, f9 numeric, f10 timestamp, f11 float4, f12 float8) partition by list (f6);

drop table if exists crypt_schema_partition_range.crypt_schema_partition_range_bj;
create table crypt_schema_partition_range.crypt_schema_partition_range_bj(f1 int, f2 int2, f3 int4, f4 int8, f5 varchar, f6 text, f7 char(20), f8 varchar2, f9 numeric, f10 timestamp, f11 float4, f12 float8) partition by range (f10);

drop table if exists crypt_schema_partition_range.crypt_schema_partition_range_gd_201702;
create table crypt_schema_partition_range.crypt_schema_partition_range_gd_201702(f1 int, f2 int2, f3 int4, f4 int8, f5 varchar, f6 text, f7 char(20), f8 varchar2, f9 numeric, f10 timestamp, f11 float4, f12 float8);
drop table if exists crypt_schema_partition_range.crypt_schema_partition_range_bj_201702;
create table crypt_schema_partition_range.crypt_schema_partition_range_bj_201702(f1 int, f2 int2, f3 int4, f4 int8, f5 varchar, f6 text, f7 char(20), f8 varchar2, f9 numeric, f10 timestamp, f11 float4, f12 float8);

-- create first level sub partition
create table crypt_schema_partition_range.crypt_schema_partition_range_gd partition of crypt_schema_partition_range.crypt_schema_partition_range for values in ('guangdong') partition by range (f10);

-- attach first level sub partition
alter table crypt_schema_partition_range.crypt_schema_partition_range attach partition crypt_schema_partition_range.crypt_schema_partition_range_bj for values in ('beijing');

-- create second level sub partition
create table crypt_schema_partition_range.crypt_schema_partition_range_gd_201701 partition of crypt_schema_partition_range.crypt_schema_partition_range_gd(f1 primary key,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12) for values from ('2017-01-01') to ('2017-02-01'); 
create table crypt_schema_partition_range.crypt_schema_partition_range_bj_201701 partition of crypt_schema_partition_range.crypt_schema_partition_range_bj(f1 primary key,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12) for values from ('2017-01-01') to ('2017-02-01');

-- attach second level sub partition
alter table crypt_schema_partition_range.crypt_schema_partition_range_bj attach partition crypt_schema_partition_range.crypt_schema_partition_range_bj_201702 for values from ('2017-02-01') to ('2017-03-01');
alter table crypt_schema_partition_range.crypt_schema_partition_range_gd attach partition crypt_schema_partition_range.crypt_schema_partition_range_gd_201702 for values from ('2017-02-01') to ('2017-03-01');

-- 
\c - mls_admin 
select attnum,algorithm_id,spcoid,spcname,nspname,tblname from pg_transparent_crypt_policy_map where tblname ilike 'crypt_schema_partition_range%' order by tblname asc;

\c - godlike
drop table crypt_schema_partition_range.crypt_schema_partition_range ;

-- there shoule be nothing left
\c - mls_admin 
select attnum,algorithm_id,spcoid,spcname,nspname,tblname from pg_transparent_crypt_policy_map where tblname ilike 'crypt_schema_partition_range%' order by tblname asc;
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_SCHEMA('crypt_schema_partition_range');

--case 7 schema pg_toast with crypt 
\c - mls_admin 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_SCHEMA('pg_toast', 4);

\c - godlike
create table crypt_default_schema(f1 int, f2 int2, f3 int4, f4 int8, f5 varchar, f6 text, f7 char(20), f8 varchar2, f9 numeric, f10 timestamp, f11 float4, f12 float8) distribute by replication;

\c - mls_admin 
select attnum,algorithm_id,spcoid,spcname,nspname from pg_transparent_crypt_policy_map where nspname ilike 'pg_toast%' order by tblname asc;
\c - godlike
drop table crypt_default_schema ;
--there shoule be no crypt storage in pg_toast schema 
\c - mls_admin 
select attnum,algorithm_id,spcoid,spcname,nspname from pg_transparent_crypt_policy_map where nspname ilike 'pg_toast%' order by tblname asc;
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_SCHEMA('pg_toast');

--end--------------------------------------------------------------

--------------------------test block:alter table schema begin--------------------------
--  | case id | table crypted | src schema crypted | dst schema crypted | result |
--  | case1   | yes           | no                 | yes/no             | fail   |
--  | case2   | yes           | yes                | yes/no             | ok     |
--  | case3   | no            | yes                | yes/no             | ok     |
--  | case4   | no            | no                 | yes/no             | ok     |
--  


\c - godlike 
create schema no_crypted_schema_alt;
create schema no_crypted_schema_alt_2;
create schema crypted_schema_alt;
create schema crypted_schema_alt_2;
create table no_crypted_schema_alt.tbl_crypted_alt(f1 int, f2 timestamp default now(), f3 int) 
partition by range (f2) 
    begin (timestamp without time zone '2018-01-01 0:0:0') step (interval '1 month') 
    partitions (3) 
    distribute by shard(f1) 
to group default_group ; 
create table no_crypted_schema_alt.tbl_nocrypt_alt(f1 int, f2 timestamp default now(), f3 int) 
partition by range (f2) 
    begin (timestamp without time zone '2018-01-01 0:0:0') step (interval '1 month') 
    partitions (3) 
    distribute by shard(f1) 
to group default_group ;
create table crypted_schema_alt.tbl_nocrypt_alt_2(f1 int, f2 timestamp default now(), f3 int) 
partition by range (f2) 
    begin (timestamp without time zone '2018-01-01 0:0:0') step (interval '1 month') 
    partitions (3) 
    distribute by shard(f1) 
to group default_group ;
\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_TABLE('no_crypted_schema_alt', 'tbl_crypted_alt', 4); 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_SCHEMA('crypted_schema_alt', 4);
select MLS_TRANSPARENT_CRYPT_ALGORITHM_BIND_SCHEMA('crypted_schema_alt_2', 4);

\c - godlike 
create table crypted_schema_alt.tbl_crypt_alt_2(f1 int, f2 timestamp default now(), f3 int) 
partition by range (f2) 
    begin (timestamp without time zone '2018-01-01 0:0:0') step (interval '1 month') 
    partitions (3) 
    distribute by shard(f1) 
to group default_group ; 

\c - mls_admin
select algorithm_id, nspname, tblname from pg_transparent_crypt_policy_map where nspname ilike '%alt%' order by 1,2,3;

--case 1 | case1   | yes           | no                 | yes/no             | fail   |  
\c - godlike
--fail 
alter table no_crypted_schema_alt.tbl_crypted_alt set schema no_crypted_schema_alt_2;
alter table no_crypted_schema_alt.tbl_crypted_alt set schema crypted_schema_alt;

--case 2 | case2   | yes           | yes                | yes/no             | ok     |
--ok 
\c - godlike 
alter table crypted_schema_alt.tbl_crypt_alt_2 set schema crypted_schema_alt_2;
\c - mls_admin
select algorithm_id, nspname, tblname from pg_transparent_crypt_policy_map where nspname ilike '%alt%' order by 1,2,3;
\c - godlike 
alter table crypted_schema_alt_2.tbl_crypt_alt_2 set schema crypted_schema_alt;
\c - mls_admin
select algorithm_id, nspname, tblname from pg_transparent_crypt_policy_map where nspname ilike '%alt%' order by 1,2,3;

--ok
\c - godlike
alter table crypted_schema_alt.tbl_crypt_alt_2 set schema no_crypted_schema_alt_2;
\c - mls_admin
select algorithm_id, nspname, tblname from pg_transparent_crypt_policy_map where nspname ilike '%alt%' order by 1,2,3;
\c - godlike
--while, can not move back
alter table no_crypted_schema_alt_2.tbl_crypt_alt_2 set schema crypted_schema_alt;
\c - mls_admin
select algorithm_id, nspname, tblname from pg_transparent_crypt_policy_map where nspname ilike '%alt%' order by 1,2,3;

--case 3 | case3   | no            | yes                | yes/no             | ok     |
--ok 
\c - godlike 
alter table crypted_schema_alt.tbl_nocrypt_alt_2 set schema crypted_schema_alt_2;
\c - godlike 
alter table crypted_schema_alt_2.tbl_nocrypt_alt_2 set schema crypted_schema_alt;

--ok
\c - godlike
alter table crypted_schema_alt.tbl_nocrypt_alt_2 set schema no_crypted_schema_alt_2;
\c - godlike
alter table no_crypted_schema_alt_2.tbl_nocrypt_alt_2 set schema crypted_schema_alt;

--case 4 | case4   | no            | no                 | yes/no             | ok     |
--ok 
\c - godlike 
alter table crypted_schema_alt.tbl_nocrypt_alt_2 set schema crypted_schema_alt_2;
\c - godlike 
alter table crypted_schema_alt_2.tbl_nocrypt_alt_2 set schema crypted_schema_alt;

--ok
\c - godlike
alter table crypted_schema_alt.tbl_nocrypt_alt_2 set schema no_crypted_schema_alt_2;
\c - godlike
alter table no_crypted_schema_alt_2.tbl_nocrypt_alt_2 set schema crypted_schema_alt;

--clean 
\c - mls_admin
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('no_crypted_schema_alt', 'tbl_crypted_alt');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('no_crypted_schema_alt_2', 'tbl_crypt_alt_2');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_SCHEMA('crypted_schema_alt');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_SCHEMA('crypted_schema_alt_2');

-- child table has bind
select algorithm_id, nspname, tblname from pg_transparent_crypt_policy_map where nspname ilike '%alt%' order by 1,2,3;

--clean child
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('no_crypted_schema_alt_2', 'tbl_crypt_alt_2_part_0');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('no_crypted_schema_alt_2', 'tbl_crypt_alt_2_part_1');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('no_crypted_schema_alt_2', 'tbl_crypt_alt_2_part_2');

\c - godlike
drop table no_crypted_schema_alt.tbl_crypted_alt;
drop table no_crypted_schema_alt.tbl_nocrypt_alt;
drop table crypted_schema_alt.tbl_nocrypt_alt_2;
drop table no_crypted_schema_alt_2.tbl_crypt_alt_2;
drop schema no_crypted_schema_alt;
drop schema no_crypted_schema_alt_2;
drop schema crypted_schema_alt;
drop schema crypted_schema_alt_2;
--------------------------test block:alter table schema end--------------------------


\c regression godlike
drop  table xx;
drop  table yy;
drop  table zz;
drop  table tbl_complex;
drop  table tbl_transparent_unsupport;
drop  table tbl_complex_guomin_333;

truncate tbl_complex;

\c - mls_admin 
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'i');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'x');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'j');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'y');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'k');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'z');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'o');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 't');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'f4');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'f8');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'n1');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'var2');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'c1');
select MLS_TRANSPARENT_CRYPT_ALGORITHM_UNBIND_TABLE('public', 'tbl_complex', 'c2');

\c - godlike
drop  table tbl_complex;

-----------------TRANSPARENT CRYPT TEST END--------------------



-------------------CLS BEGIN--------------------
\c - godlike
create table xixi( i int primary key, j int) distribute by shard(i);
create table momo( i int, j int) distribute by shard(i);
create user busboy superuser;
create user badboy superuser;
--create user godlike superuser;
--create user rubberneck superuser;

\c - mls_admin
select MLS_CLS_CREATE_POLICY('cls_compare', 99);

select MLS_CLS_CREATE_LEVEL('cls_compare', 10, 'defaultlevel', 'default level');
select MLS_CLS_CREATE_LEVEL('cls_compare', 10, 'userlevel', 'user level');
select MLS_CLS_CREATE_LEVEL('cls_compare', 9,  'badlevel', 'bad level');

select MLS_CLS_CREATE_COMPARTMENT('cls_compare', 30, 'a_com', 'a_com');
select MLS_CLS_CREATE_COMPARTMENT('cls_compare', 31, 'b_com', 'b_com');
select MLS_CLS_CREATE_COMPARTMENT('cls_compare', 32, 'c_com', 'c_com');

select MLS_CLS_CREATE_GROUP_ROOT('cls_compare', 51, 'root', 'root');
--fail
select MLS_CLS_CREATE_GROUP_ROOT('cls_compare', 50, 'child_level_2', 'child_level_2');
--fail
select MLS_CLS_CREATE_GROUP_NODE('cls_compare', 50, 'child_level_2', 'child_level_2', 'child_level_1');
--ok
select MLS_CLS_CREATE_GROUP_NODE('cls_compare', 52, 'child_level_1', 'child_level_1', 'root');
select MLS_CLS_CREATE_GROUP_NODE('cls_compare', 50, 'child_level_2', 'child_level_2', 'child_level_1');
select MLS_CLS_CREATE_GROUP_NODE('cls_compare', 54, 'child_2_level_2', 'child_2_level_2', 'child_level_1');

--default label
select MLS_CLS_CREATE_LABEL('cls_compare', 1024, 'defaultlevel:a_com:child_level_2');
select MLS_CLS_CREATE_LABEL('cls_compare', 1025, 'userlevel::');
--bad level
select MLS_CLS_CREATE_LABEL('cls_compare', 1026, 'badlevel::');
select MLS_CLS_CREATE_LABEL('cls_compare', 2048, 'defaultlevel:a_com:');
select MLS_CLS_CREATE_LABEL('cls_compare', 2049, 'defaultlevel:b_com,c_com,a_com:');
--bad compartment
select MLS_CLS_CREATE_LABEL('cls_compare', 2050, 'defaultlevel:b_com,c_com:');
select MLS_CLS_CREATE_LABEL('cls_compare', 3096, 'defaultlevel:a_com:child_level_2');
select MLS_CLS_CREATE_LABEL('cls_compare', 3097, 'defaultlevel:a_com:root');
select MLS_CLS_CREATE_LABEL('cls_compare', 3098, 'defaultlevel:a_com:child_level_2,child_2_level_2');
--bad group
select MLS_CLS_CREATE_LABEL('cls_compare', 3099, 'defaultlevel:a_com:child_2_level_2');

\c - godlike
--fail
alter table public.xixi add column _cls clsitem default '99:1024';

\c - mls_admin
--temp treat, later this action would be embeded in MLS_CLS_CREATE_TABLE_LABEL.
--ok
alter table public.xixi add column _cls clsitem default '99:1024';
--temp treat
--fail
alter table public.xixi rename _cls to __cls;

\c - godlike
--fail
alter table public.xixi drop column _cls;
alter table public.xixi rename _cls to __cls;

--go to cases
\c - mls_admin
select MLS_CLS_CREATE_TABLE_LABEL('cls_compare', 1024, 'public', 'xixi');

--ROUND1. level judge
--define user label
select MLS_CLS_DROP_USER_LABEL('godlike');
select MLS_CLS_DROP_USER_LABEL('busboy');
select MLS_CLS_DROP_USER_LABEL('badboy');
select MLS_CLS_CREATE_USER_LABEL('cls_compare', 'godlike', 1025, 1025, 1025);
select MLS_CLS_CREATE_USER_LABEL('cls_compare', 'busboy',  1024, 1024, 1024);
select MLS_CLS_CREATE_USER_LABEL('cls_compare', 'badboy',  1026, 1026, 1026);
--while, rubberneck has no label
select pgxc_pool_disconnect('', 'godlike');
select pgxc_pool_disconnect('', 'busboy');
select pgxc_pool_disconnect('', 'badboy');

--case: insert with row label
--_cls with 99:1025
\c - godlike
insert into xixi select generate_series(1,5), generate_series(1,5);
select * from xixi order by i;
--insert sucess, but select nothing
\c - rubberneck
insert into xixi select generate_series(6,10), generate_series(6,10);
select * from xixi order by i;
--lower level, see row inserted by himself, 99:1026
\c - badboy
insert into xixi select generate_series(11,15), generate_series(11,15);
select * from xixi order by i;
--rows created by badboy with 99:1026. without rows created by rubberneck _cls with 99:1024, cause, default label having compartment and group, so check them later.
\c - godlike
select * from xixi order by i;

--case:select with read label
--insert ok
\c - godlike
insert into momo select i,j from xixi;
select * from momo order by i;
truncate momo;
--insert nothing, cause rubberneck dose not have cls label bound, so rows selected from xixi.
\c - rubberneck
insert into momo select i,j from xixi;
--momo is empty
select * from momo order by i;
truncate momo;
--fetch rows inserted by himself
\c - badboy
insert into momo select i,j from xixi;
select * from momo order by i;
truncate momo;

--case: select for update with read label
--there is one record
\c - godlike
select * from xixi where i  =2 for UPDATE;
--get nothing
\c - rubberneck
select * from xixi where i  =2 for UPDATE;
--get nothing
\c - badboy
select * from xixi where i  =2 for UPDATE;
--get rows inserted by himself
select * from xixi where i  =11 for UPDATE;

--case: select for share with read label
--there is one record
\c - godlike
select * from xixi where i  =2 for SHARE;
--get nothing
\c - rubberneck
select * from xixi where i  =2 for SHARE;
--get nothing
\c - badboy
select * from xixi where i  =2 for SHARE;
--get rows inserted by himself
select * from xixi where i  =11 for SHARE;

--case: create table as select ..., read label and insert label
--_cls with 99:1025
\c - godlike
set default_locator_type = 'shard';
create table lala as select * from xixi;
select * from lala order by i;
--empty table
\c - rubberneck
set default_locator_type = 'shard';
create table lala2 as select * from xixi;
select * from lala2 order by i;
--get rows inserted by himself
\c - badboy
set default_locator_type = 'shard';
create table lala3 as select * from xixi;
select * from lala3 order by i;

--case: update with write label
--success
\c - godlike
select * from xixi where i = 1;
update xixi set j = 3344 where i = 1;
select * from xixi where i = 1;
--update fails
\c - rubberneck
update xixi set j = 4433 where i = 1;
\c - godlike
select * from xixi where i = 1;
--update fails
\c - badboy
update xixi set j = 4433 where i = 1;
\c - godlike
select * from xixi where i = 1;
--update success
\c - badboy
select * from xixi where i = 11;
update xixi set j = 4433 where i = 11;
\c - godlike
select * from xixi where i = 11;

--case: delete with write label
--success
\c - godlike
delete from xixi where i = 1;
select * from xixi where i = 1;
insert into xixi values(1, 1);
--fails
\c - rubberneck
delete from xixi where i = 2;
-- i = 2 still exists
\c - godlike
select * from xixi where i = 2;
--fails
\c - badboy
delete from xixi where i = 2;
-- i = 2 still exists
\c - godlike
select * from xixi where i = 2;
--success
\c - badboy
delete from xixi where i = 11;
\c - godlike
select * from xixi where i = 11;
\c - badboy
insert into xixi values(11,11);
select * from xixi order by i;

--case: upsert, do update
--new _cls label also is 99:1025, update sucess
\c - godlike
insert into xixi values(1,1) on conflict(i) do update set j = 9988;
select * from xixi where i = 1;
--update fails, cause rubberneck dose not have cls bound
\c - rubberneck
insert into xixi values(1,1) on conflict(i) do update set j = 8899;
\c - godlike
select * from xixi where i = 1;
--update fails, cause badboy have lower level
\c - badboy
insert into xixi values(1,1) on conflict(i) do update set j = 8899;
\c - godlike
select * from xixi where i = 1;
--update success, row inserted by himself
\c - badboy
select * from xixi where i = 12;
insert into xixi values(12,12) on conflict(i) do update set j = 8899;
select * from xixi where i = 12;

--case: update, do insert
--_cls with 99:1025
\c - godlike
insert into xixi values(101,101) on conflict(i) do update set j = 9988;
select * from xixi where i = 101;
--_cls with 99:1024
\c - rubberneck
insert into xixi values(102,102) on conflict(i) do update set j = 8899;
--the row inserted by rubbneck can not be seen
\c - godlike
select * from xixi where i = 102;
--_cls with 99:1026
\c - badboy
insert into xixi values(103,103) on conflict(i) do update set j = 8899;
\c - godlike
select * from xixi where i = 103;

--case: upsert with where condition
--_cls keep orignal 99:1025, update success
\c - godlike
--update to 1024
insert into xixi as x(i,j) values(3,3) on conflict(i) do update set j = 1024 where x.i = 3;
select * from xixi where i = 3;
--update to 2048
insert into xixi as x(i,j) values(3,3) on conflict(i) do update set j = 2048 where x.j = 1024 and x.i = 3;
select * from xixi where i = 3;
\c - badboy
--fails to update 
insert into xixi as x(i,j) values(6,6) on conflict(i) do update set j = 3096 where x.j = 2048 and x.i = 6;
select * from xixi where i = 6;
\c - godlike
select * from xixi where i = 6;
--update success, cause rows inserted by himself
\c - badboy
insert into xixi as x(i,j) values(11,11) on conflict(i) do update set j = 3096 where x.j = 11 and x.i = 11;
select * from xixi where i = 11;
\c - godlike
select * from xixi where i = 11;

--case: insert into select from join 
\c - godlike 
insert into lala2 select a.i,a.j,b._cls from lala a, lala3 b where a.i = b.i;
select * from lala2 order by i;

--ROUND1. end
truncate table xixi;
truncate table momo;
truncate table lala;
truncate table lala2;
truncate table lala3;
drop table lala;
drop table lala2;
drop table lala3;


--ROUND2. compartment judge
--define user label
\c - mls_admin
select MLS_CLS_DROP_USER_LABEL('godlike');
select MLS_CLS_DROP_USER_LABEL('busboy');
select MLS_CLS_DROP_USER_LABEL('badboy');
select MLS_CLS_CREATE_USER_LABEL('cls_compare', 'godlike', 2049, 2049, 2049);
select MLS_CLS_CREATE_USER_LABEL('cls_compare', 'busboy',  2048, 2048, 2048);
select MLS_CLS_CREATE_USER_LABEL('cls_compare', 'badboy',  2050, 2050, 2050);
--while, rubberneck has no label
select pgxc_pool_disconnect('', 'godlike');
select pgxc_pool_disconnect('', 'busboy');
select pgxc_pool_disconnect('', 'badboy');

--case: insert with row label
--_cls with 99:2049
\c - godlike
insert into xixi select generate_series(1,5), generate_series(1,5);
select * from xixi order by i;
--insert sucess, but select nothing
\c - rubberneck
insert into xixi select generate_series(6,10), generate_series(6,10);
select * from xixi order by i;
--different compartment, see row inserted by himself, 99:2050
\c - badboy
insert into xixi select generate_series(11,15), generate_series(11,15);
select * from xixi order by i;

--case:select with read label
--insert ok
\c - godlike
insert into momo select i,j from xixi;
select * from momo order by i;
truncate momo;
--insert nothing, cause rubberneck dose not have cls label bound, so rows selected from xixi.
\c - rubberneck
insert into momo select i,j from xixi;
--momo is empty
select * from momo order by i;
truncate momo;
--fetch rows inserted by himself
\c - badboy
insert into momo select i,j from xixi;
select * from momo order by i;
truncate momo;

--case: select for update with read label
--there is one record
\c - godlike
select * from xixi where i  =2 for UPDATE;
--get nothing
\c - rubberneck
select * from xixi where i  =2 for UPDATE;
--get nothing
\c - badboy
select * from xixi where i  =2 for UPDATE;
--get rows inserted by himself
select * from xixi where i  =11 for UPDATE;

--case: select for share with read label
--there is one record
\c - godlike
select * from xixi where i  =2 for SHARE;
--get nothing
\c - rubberneck
select * from xixi where i  =2 for SHARE;
--get nothing
\c - badboy
select * from xixi where i  =2 for SHARE;
--get rows inserted by himself
select * from xixi where i  =11 for SHARE;

--case: create table as select ..., read label and insert label
--_cls with 99:2049
\c - godlike
set default_locator_type = 'shard';
create table lala as select * from xixi;
select * from lala order by i;
--empty table
\c - rubberneck
set default_locator_type = 'shard';
create table lala2 as select * from xixi;
select * from lala2 order by i;
--get rows inserted by himself
\c - badboy
set default_locator_type = 'shard';
create table lala3 as select * from xixi;
select * from lala3 order by i;

--case: update with write label
--success
\c - godlike
select * from xixi where i = 1;
update xixi set j = 3344 where i = 1;
select * from xixi where i = 1;
--update fails
\c - rubberneck
update xixi set j = 4433 where i = 1;
\c - godlike
select * from xixi where i = 1;
--update fails
\c - badboy
update xixi set j = 4433 where i = 1;
\c - godlike
select * from xixi where i = 1;
--update success
\c - badboy
select * from xixi where i = 11;
update xixi set j = 4433 where i = 11;
select * from xixi where i = 11;

--case: delete with write label
--success
\c - godlike
delete from xixi where i = 1;
select * from xixi where i = 1;
insert into xixi values(1, 1);
--fails
\c - rubberneck
delete from xixi where i = 2;
-- i = 2 still exists
\c - godlike
select * from xixi where i = 2;
--fails
\c - badboy
delete from xixi where i = 2;
-- i = 2 still exists
\c - godlike
select * from xixi where i = 2;
--success
\c - badboy
delete from xixi where i = 11;
select * from xixi where i = 11;
insert into xixi values(11, 111);
select * from xixi where i = 11;

--case: upsert, do update
--new _cls label also is 99:2049, update sucess
\c - godlike
select * from xixi where i = 1;
insert into xixi values(1,1) on conflict(i) do update set j = 9988;
select * from xixi where i = 1;
--update fails, cause rubberneck dose not have cls bound
\c - rubberneck
insert into xixi values(1,1) on conflict(i) do update set j = 8899;
\c - godlike
select * from xixi where i = 1;
--update fails, cause badboy have lower level
\c - badboy
insert into xixi values(1,1) on conflict(i) do update set j = 8899;
\c - godlike
select * from xixi where i = 1;

--case: update, do insert
--_cls with 99:2049
\c - godlike
insert into xixi values(101,101) on conflict(i) do update set j = 9988;
select * from xixi where i = 101;
--_cls with 99:1024
\c - rubberneck
insert into xixi values(102,102) on conflict(i) do update set j = 8899;
\c - godlike
select * from xixi where i = 102;
--_cls with 99:2050
\c - badboy
insert into xixi values(103,103) on conflict(i) do update set j = 8899;
select * from xixi where i = 103;

--case: upsert with where condition
--_cls keep orignal 99:2049, update success
\c - godlike
--update to 1024
insert into xixi as x(i,j) values(3,3) on conflict(i) do update set j = 1024 where x.i = 3;
select * from xixi where i = 3;
--update to 2048
insert into xixi as x(i,j) values(3,3) on conflict(i) do update set j = 2048 where x.j = 1024 and x.i = 3;
select * from xixi where i = 3;
\c - badboy
--fails to update
insert into xixi as x(i,j) values(6,6) on conflict(i) do update set j = 3096 where x.j = 2048 and x.i = 6;
select * from xixi where i = 6;
\c - godlike
select * from xixi where i = 6;
--update success, cause rows inserted by himself
\c - badboy
insert into xixi as x(i,j) values(11,11) on conflict(i) do update set j = 3096 where x.j = 11 and x.i = 11;
select * from xixi where i = 11;
select * from xixi where i = 11;


--ROUND2. end
truncate table xixi;
truncate table momo;
truncate table lala;
truncate table lala2;
truncate table lala3;
drop table lala;
drop table lala2;
drop table lala3;

--ROUND3. group judge
--define user label
\c - mls_admin
select MLS_CLS_DROP_USER_LABEL('godlike');
select MLS_CLS_DROP_USER_LABEL('busboy');
select MLS_CLS_DROP_USER_LABEL('badboy');
select MLS_CLS_CREATE_USER_LABEL('cls_compare', 'godlike', 3097, 3097, 3097);
select MLS_CLS_CREATE_USER_LABEL('cls_compare', 'busboy',  3096, 3096, 3096);
select MLS_CLS_CREATE_USER_LABEL('cls_compare', 'badboy',  3099, 3099, 3099);
--while, rubberneck has no label
select pgxc_pool_disconnect('', 'godlike');
select pgxc_pool_disconnect('', 'busboy');
select pgxc_pool_disconnect('', 'badboy');

--case: insert with row label
--_cls with 99:3097
\c - godlike
insert into xixi select generate_series(1,5), generate_series(1,5);
select * from xixi order by i;
--insert sucess, but select nothing
\c - rubberneck
insert into xixi select generate_series(6,10), generate_series(6,10);
select * from xixi order by i;
--lower level, see row inserted by himself, 99:3099
\c - badboy
insert into xixi select generate_series(11,15), generate_series(11,15);
select * from xixi order by i;
--rows created by rubberneck _cls with 99:1024, and created by badboy with 99:3099
\c - godlike
select * from xixi order by i;

--case:select with read label
--insert ok
\c - godlike
insert into momo select i,j from xixi;
select * from momo order by i;
truncate momo;
--insert nothing, cause rubberneck dose not have cls label bound, so rows selected from xixi.
\c - rubberneck
insert into momo select i,j from xixi;
--momo is empty
select * from momo order by i;
truncate momo;
--fetch rows inserted by himself
\c - badboy
insert into momo select i,j from xixi;
select * from momo order by i;
truncate momo;

--case: select for update with read label
--there is one record
\c - godlike
select * from xixi where i  =2 for UPDATE;
--get nothing
\c - rubberneck
select * from xixi where i  =2 for UPDATE;
--get nothing
\c - badboy
select * from xixi where i  =2 for UPDATE;
--get rows inserted by himself
select * from xixi where i  =11 for UPDATE;

--case: select for share with read label
--there is one record
\c - godlike
select * from xixi where i  =2 for SHARE;
--get nothing
\c - rubberneck
select * from xixi where i  =2 for SHARE;
--get nothing
\c - badboy
select * from xixi where i  =2 for SHARE;
--get rows inserted by himself
select * from xixi where i  =11 for SHARE;

--case: create table as select ..., read label and insert label
--_cls with 99:3097
\c - godlike
set default_locator_type = 'shard';
create table lala as select * from xixi;
select * from lala order by i;
--empty table
\c - rubberneck
set default_locator_type = 'shard';
create table lala2 as select * from xixi;
select * from lala2 order by i;
--get rows inserted by himself
\c - badboy
set default_locator_type = 'shard';
create table lala3 as select * from xixi;
select * from lala3 order by i;

--case: update with write label
--success
\c - godlike
select * from xixi where i = 1;
update xixi set j = 3344 where i = 1;
select * from xixi where i = 1;
--update fails
\c - rubberneck
update xixi set j = 4433 where i = 1;
\c - godlike
select * from xixi where i = 1;
--update fails
\c - badboy
update xixi set j = 4433 where i = 1;
\c - godlike
select * from xixi where i = 1;
--update success
\c - badboy
update xixi set j = 4433 where i = 11;
\c - godlike
select * from xixi where i = 11;

--case: delete with write label
--success
\c - godlike
delete from xixi where i = 1;
select * from xixi where i = 1;
insert into xixi values(1, 1);
--fails
\c - rubberneck
delete from xixi where i = 2;
-- i = 2 still exists
\c - godlike
select * from xixi where i = 2;
--fails
\c - badboy
delete from xixi where i = 2;
-- i = 2 still exists
\c - godlike
select * from xixi where i = 2;
--success
\c - badboy
delete from xixi where i = 11;
\c - godlike
select * from xixi where i = 11;
\c - badboy
insert into xixi values(11, 111);
select * from xixi where i = 11;
\c - godlike
select * from xixi where i = 11;

--case: upsert, do update
--new _cls label also is 99:3097, update sucess
\c - godlike
insert into xixi values(1,1) on conflict(i) do update set j = 9988;
select * from xixi where i = 1;
--update fails, cause rubberneck dose not have cls bound
\c - rubberneck
insert into xixi values(1,1) on conflict(i) do update set j = 8899;
\c - godlike
select * from xixi where i = 1;
--update fails, cause badboy have lower level
\c - badboy
insert into xixi values(1,1) on conflict(i) do update set j = 8899;
\c - godlike
select * from xixi where i = 1;

--case: update, do insert
--_cls with 99:3097
\c - godlike
insert into xixi values(101,101) on conflict(i) do update set j = 9988;
select * from xixi where i = 101;
--_cls with 99:1024
\c - rubberneck
insert into xixi values(102,102) on conflict(i) do update set j = 8899;
\c - godlike
select * from xixi where i = 102;
--_cls with 99:3099
\c - badboy
insert into xixi values(103,103) on conflict(i) do update set j = 8899;
\c - godlike
select * from xixi where i = 103;

--case: upsert with where condition
--_cls keep orignal 99:3097, update success
\c - godlike
--update to 1024
insert into xixi as x(i,j) values(6,6) on conflict(i) do update set j = 1024 where x.i = 6;
select * from xixi where i = 6;
--update to 2048
insert into xixi as x(i,j) values(6,6) on conflict(i) do update set j = 2048 where x.j = 1024 and x.i = 6;
select * from xixi where i = 6;
\c - badboy
--fails to update 
insert into xixi as x(i,j) values(6,6) on conflict(i) do update set j = 3096 where x.j = 2048 and x.i = 6;
select * from xixi where i = 6;
\c - godlike
select * from xixi where i = 6;
--update success, cause rows inserted by himself
\c - badboy
insert into xixi as x(i,j) values(11,11) on conflict(i) do update set j = 3096 where x.j = 111 and x.i = 11;
select * from xixi where i = 11;
\c - godlike
select * from xixi where i = 11;


--ROUND3. end
truncate table xixi;
truncate table momo;
truncate table lala;
truncate table lala2;
truncate table lala3;
drop table lala;
drop table lala2;
drop table lala3;

\c - mls_admin
select polid, attnum, enable, nspname, tblname, reloptions from pg_cls_table;
select MLS_CLS_DROP_TABLE_LABEL('cls_compare', 'public', 'xixi');
select polid, attnum, enable, nspname, tblname, reloptions from pg_cls_table;

--everything is done
\c - godlike
drop table xixi;
drop table momo;

-----------------CLS END--------------------

