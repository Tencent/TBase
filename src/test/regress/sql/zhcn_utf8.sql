--
-- gbk
--
CREATE DATABASE db_gbk template template0 encoding = gbk LC_COLLATE = 'zh_CN.gbk' LC_CTYPE = 'zh_CN.gbk';
\c db_gbk;

CREATE TABLE tbl_gbk(f1 varchar(3));
INSERT INTO tbl_gbk (f1) VALUES ('邓东宝');
INSERT INTO tbl_gbk (f1) VALUES ('李尔王');
-- 镕 is not support by euc_cn, but support on gbk
INSERT INTO tbl_gbk (f1) VALUES ('朱镕非');
INSERT INTO tbl_gbk (f1) VALUES ('王家坝');
INSERT INTO tbl_gbk (f1) VALUES ('王一位');
INSERT INTO tbl_gbk (f1) VALUES ('怡宝');
-- error
INSERT INTO tbl_gbk (f1) VALUES ('王家坝2');

-- order by
SELECT * FROM tbl_gbk ORDER BY f1;

-- regular expression query
SELECT * FROM tbl_gbk WHERE f1 ~ '^王' ORDER BY f1;

-- query encoding length
SELECT OCTET_LENGTH(f1) FROM tbl_gbk ORDER BY f1;

-- MATERIALIZED VIEW join
CREATE TABLE T_PERSON(i int, n varchar(32));
INSERT INTO T_PERSON VALUES (1, '韩梅梅');
INSERT INTO T_PERSON VALUES (2, '张雷');
CREATE TABLE T_NICK(id int, name varchar(32));
INSERT INTO T_NICK VALUES (1, '叶子');
INSERT INTO T_NICK VALUES (2, '蓝天');
CREATE MATERIALIZED VIEW T_MATER AS SELECT * FROM T_PERSON  WITH NO DATA;
REFRESH MATERIALIZED VIEW T_MATER;
SELECT * FROM T_MATER p JOIN T_NICK n on p.i = n.id order by i;
SELECT * FROM T_MATER p JOIN T_NICK n on p.i = n.id order by name;
SELECT * FROM T_MATER p JOIN T_NICK n on p.i = n.id order by n;
DROP MATERIALIZED VIEW T_MATER;
DROP TABLE T_PERSON;
DROP TABLE T_NICK;

--
-- gb18030
--
CREATE DATABASE db_gb18030 template template0 encoding = gb18030 LC_COLLATE = 'zh_CN.gb18030' LC_CTYPE = 'zh_CN.gb18030';
\c db_gb18030;

CREATE TABLE tbl_gb18030(f1 varchar(3));
INSERT INTO tbl_gb18030 (f1) VALUES ('邓东宝');
INSERT INTO tbl_gb18030 (f1) VALUES ('李尔王');
-- 镕 is not support by euc_cn, but support on gb18030
INSERT INTO tbl_gb18030 (f1) VALUES ('朱镕非');
INSERT INTO tbl_gb18030 (f1) VALUES ('王家坝');
INSERT INTO tbl_gb18030 (f1) VALUES ('王一位');
INSERT INTO tbl_gb18030 (f1) VALUES ('怡宝');
-- which not support by gbk, but support on gb18030
INSERT INTO tbl_gb18030 (f1) VALUES ('𣘗𧄧');
-- out of bound error
INSERT INTO tbl_gb18030 (f1) VALUES ('王家坝2');
INSERT INTO tbl_gb18030 (f1) VALUES ('𣘗𧄧2');

-- text
CREATE TABLE tbl_text(i int, f1 text);
INSERT INTO tbl_text (f1) VALUES ('邓东宝');
INSERT INTO tbl_text (f1) VALUES ('李尔王');
-- 镕 is not support by euc_cn, but support on gb18030
INSERT INTO tbl_text (f1) VALUES ('朱镕非');
INSERT INTO tbl_text (f1) VALUES ('王家坝');
INSERT INTO tbl_text (f1) VALUES ('王一位');
INSERT INTO tbl_text (f1) VALUES ('怡宝');
-- which not support by gbk, but support on gb18030
INSERT INTO tbl_text (f1) VALUES ('𣘗𧄧');
SELECT * FROM tbl_text ORDER BY f1;

-- nvarchar2
CREATE TABLE tbl_nvarchar2(i int, f1 nvarchar2(3) );
INSERT INTO tbl_nvarchar2 (f1) VALUES ('邓东宝');
INSERT INTO tbl_nvarchar2 (f1) VALUES ('李尔王');
-- 镕 is not support by euc_cn, but support on gb18030
INSERT INTO tbl_nvarchar2 (f1) VALUES ('朱镕非');
INSERT INTO tbl_nvarchar2 (f1) VALUES ('王家坝');
INSERT INTO tbl_nvarchar2 (f1) VALUES ('王一位');
INSERT INTO tbl_nvarchar2 (f1) VALUES ('怡宝');
-- which not support by gbk, but support on gb18030
INSERT INTO tbl_nvarchar2 (f1) VALUES ('𣘗𧄧');
SELECT * FROM tbl_nvarchar2 ORDER BY f1;

-- bpchar
CREATE TABLE tbl_bpchar(i int, f1 bpchar(3) );
INSERT INTO tbl_bpchar (f1) VALUES ('邓东宝');
INSERT INTO tbl_bpchar (f1) VALUES ('李尔王');
-- 镕 is not support by euc_cn, but support on gb18030
INSERT INTO tbl_bpchar (f1) VALUES ('朱镕非');
INSERT INTO tbl_bpchar (f1) VALUES ('王家坝');
INSERT INTO tbl_bpchar (f1) VALUES ('王一位');
INSERT INTO tbl_bpchar (f1) VALUES ('怡宝');
-- which not support by gbk, but support on gb18030
INSERT INTO tbl_bpchar (f1) VALUES ('𣘗𧄧');
SELECT * FROM tbl_bpchar ORDER BY f1;

-- char
CREATE TABLE tbl_char(i int, f1 char(3) );
INSERT INTO tbl_char (f1) VALUES ('邓东宝');
INSERT INTO tbl_char (f1) VALUES ('李尔王');
-- 镕 is not support by euc_cn, but support on gb18030
INSERT INTO tbl_char (f1) VALUES ('朱镕非');
INSERT INTO tbl_char (f1) VALUES ('王家坝');
INSERT INTO tbl_char (f1) VALUES ('王家1');
INSERT INTO tbl_char (f1) VALUES ('王家2');
INSERT INTO tbl_char (f1) VALUES ('王一位');
INSERT INTO tbl_char (f1) VALUES ('怡宝');
-- which not support by gbk, but support on gb18030
INSERT INTO tbl_char (f1) VALUES ('𣘗𧄧');
SELECT * FROM tbl_char ORDER BY f1;

-- order by
SELECT * FROM tbl_gb18030 ORDER BY f1;

-- regular expression query
SELECT * FROM tbl_gb18030 WHERE f1 ~ '^王' ORDER BY f1;

-- query encoding length
SELECT OCTET_LENGTH(f1) FROM tbl_gb18030 ORDER BY f1;

-- MATERIALIZED VIEW join
CREATE TABLE T_PERSON(i int, n varchar(32));
INSERT INTO T_PERSON VALUES (1, '韩梅梅');
INSERT INTO T_PERSON VALUES (2, '李雷');
CREATE TABLE T_NICK(id int, name varchar(32));
INSERT INTO T_NICK VALUES (1, '叶子');
INSERT INTO T_NICK VALUES (2, '蓝天');
CREATE MATERIALIZED VIEW T_MATER AS SELECT * FROM T_PERSON  WITH NO DATA;
REFRESH MATERIALIZED VIEW T_MATER;
SELECT * FROM T_NICK n JOIN T_MATER p on n.id=p.i order by i;
SELECT * FROM T_NICK n JOIN T_MATER p on n.id=p.i order by name;
SELECT * FROM T_NICK n JOIN T_MATER p on n.id=p.i order by n;
DROP MATERIALIZED VIEW T_MATER;
DROP TABLE T_PERSON;
DROP TABLE T_NICK;
