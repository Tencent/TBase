--
-- XC_ALTER_TABLE
--

-- Check on dropped columns
CREATE TABLE xc_alter_table_1 (id serial, name varchar(80), code varchar(80)) DISTRIBUTE BY HASH(id);
EXPLAIN (VERBOSE true, COSTS false, NODES false) INSERT INTO xc_alter_table_1(name) VALUES ('aaa'),('bbb'),('ccc');
INSERT INTO xc_alter_table_1(name) VALUES ('aaa'),('bbb'),('ccc');
SELECT id, name, code FROM xc_alter_table_1 ORDER BY 1;
-- Cannot drop distribution column
ALTER TABLE xc_alter_table_1 DROP COLUMN id;
-- Drop 1st column
ALTER TABLE xc_alter_table_1 DROP COLUMN code;
-- Check for query generation of remote INSERT
INSERT INTO xc_alter_table_1(name) VALUES('ddd'),('eee'),('fff');
EXPLAIN (VERBOSE true, COSTS false, NODES false) INSERT INTO xc_alter_table_1(name) VALUES('ddd'),('eee'),('fff');
SELECT id, name FROM xc_alter_table_1 ORDER BY 1;
-- Check for query generation of remote INSERT SELECT
INSERT INTO xc_alter_table_1(name) SELECT 'ggg';
EXPLAIN (VERBOSE true, COSTS false, NODES false) INSERT INTO xc_alter_table_1(name) SELECT 'ggg';
SELECT id, name FROM xc_alter_table_1 ORDER BY 1;
-- Check for query generation of remote UPDATE
EXPLAIN (VERBOSE true, COSTS false, NODES false) UPDATE xc_alter_table_1 SET name = 'zzz' WHERE id = currval('xc_alter_table_1_id_seq');
UPDATE xc_alter_table_1 SET name = 'zzz' WHERE id = currval('xc_alter_table_1_id_seq');
SELECT id, name FROM xc_alter_table_1 ORDER BY 1;
DROP TABLE xc_alter_table_1;

-- Check for multiple columns dropped and created
CREATE TABLE xc_alter_table_2 (a int, b varchar(20), c boolean, d text, e interval) distribute by replication;
INSERT INTO xc_alter_table_2 VALUES (1, 'John', true, 'Master', '01:00:10');
INSERT INTO xc_alter_table_2 VALUES (2, 'Neo', true, 'Slave', '02:34:00');
INSERT INTO xc_alter_table_2 VALUES (3, 'James', false, 'Cascading slave', '00:12:05');
SELECT a, b, c, d, e FROM xc_alter_table_2 ORDER BY a;
-- Go through standard planner
SET enable_fast_query_shipping TO false;
-- Drop a couple of columns
ALTER TABLE xc_alter_table_2 DROP COLUMN a;
ALTER TABLE xc_alter_table_2 DROP COLUMN d;
ALTER TABLE xc_alter_table_2 DROP COLUMN e;
-- Check for query generation of remote INSERT
EXPLAIN (VERBOSE true, COSTS false, NODES false) INSERT INTO xc_alter_table_2 VALUES ('Kodek', false);
INSERT INTO xc_alter_table_2 VALUES ('Kodek', false);
SELECT b, c FROM xc_alter_table_2 ORDER BY b;
-- Check for query generation of remote UPDATE
EXPLAIN (VERBOSE true, COSTS false, NODES false) UPDATE xc_alter_table_2 SET b = 'Morphee', c = false WHERE b = 'Neo';
UPDATE xc_alter_table_2 SET b = 'Morphee', c = false WHERE b = 'Neo';
SELECT b, c FROM xc_alter_table_2 ORDER BY b;
-- Add some new columns
ALTER TABLE xc_alter_table_2 ADD COLUMN a int;
ALTER TABLE xc_alter_table_2 ADD COLUMN a2 varchar(20);
-- Check for query generation of remote INSERT
EXPLAIN (VERBOSE true, COSTS false, NODES false) INSERT INTO xc_alter_table_2 (a, a2, b, c) VALUES (100, 'CEO', 'Gordon', true);
INSERT INTO xc_alter_table_2 (a, a2, b, c) VALUES (100, 'CEO', 'Gordon', true);
SELECT a, a2, b, c FROM xc_alter_table_2 ORDER BY b;
-- Check for query generation of remote UPDATE
EXPLAIN (VERBOSE true, COSTS false, NODES false) UPDATE xc_alter_table_2 SET a = 200, a2 = 'CTO' WHERE b = 'John';
UPDATE xc_alter_table_2 SET a = 200, a2 = 'CTO' WHERE b = 'John';
SELECT a, a2, b, c FROM xc_alter_table_2 ORDER BY b;
DROP TABLE xc_alter_table_2;

-- Tests for ALTER TABLE redistribution
-- In the following test, a table is redistributed in all the ways possible
-- and effects of redistribution is checked on all the dependent objects
-- Table with integers
CREATE TABLE xc_alter_table_3 (a int, b varchar(10)) DISTRIBUTE BY HASH(a);
INSERT INTO xc_alter_table_3 VALUES (0, NULL);
INSERT INTO xc_alter_table_3 VALUES (1, 'a');
INSERT INTO xc_alter_table_3 VALUES (2, 'aa');
INSERT INTO xc_alter_table_3 VALUES (3, 'aaa');
INSERT INTO xc_alter_table_3 VALUES (4, 'aaaa');
INSERT INTO xc_alter_table_3 VALUES (5, 'aaaaa');
INSERT INTO xc_alter_table_3 VALUES (6, 'aaaaaa');
INSERT INTO xc_alter_table_3 VALUES (7, 'aaaaaaa');
INSERT INTO xc_alter_table_3 VALUES (8, 'aaaaaaaa');
INSERT INTO xc_alter_table_3 VALUES (9, 'aaaaaaaaa');
INSERT INTO xc_alter_table_3 VALUES (10, 'aaaaaaaaaa');
-- Create some objects to check the effect of redistribution
CREATE VIEW xc_alter_table_3_v AS SELECT count(*), sum(a), avg(a) FROM xc_alter_table_3;
CREATE RULE xc_alter_table_3_insert AS ON UPDATE TO xc_alter_table_3 WHERE OLD.a = 11 DO INSERT INTO xc_alter_table_3 VALUES (OLD.a + 1, 'nnn');
PREPARE xc_alter_table_insert AS INSERT INTO xc_alter_table_3 VALUES ($1, $2);
PREPARE xc_alter_table_delete AS DELETE FROM xc_alter_table_3 WHERE a = $1;
PREPARE xc_alter_table_update AS UPDATE xc_alter_table_3 SET b = $2 WHERE a = $1;

-- Now begin the tests
ALTER TABLE xc_alter_table_3 DISTRIBUTE BY HASH(a);
SELECT count(*), sum(a), avg(a) FROM xc_alter_table_3; -- Check on tuple presence
SELECT * FROM xc_alter_table_3_v;
EXECUTE xc_alter_table_insert(11, 'b');
SELECT b FROM xc_alter_table_3 WHERE a = 11;
EXECUTE xc_alter_table_update(11, 'bb');
SELECT b FROM xc_alter_table_3 WHERE a = 11;
EXECUTE xc_alter_table_delete(11);
SELECT b FROM xc_alter_table_3 WHERE a = 11 or a = 12;
EXECUTE xc_alter_table_delete(12);
ALTER TABLE xc_alter_table_3 DISTRIBUTE BY HASH(b);
SELECT count(*), sum(a), avg(a) FROM xc_alter_table_3; -- Check on tuple presence
SELECT * FROM xc_alter_table_3_v;
EXECUTE xc_alter_table_insert(11, 'b');
SELECT b FROM xc_alter_table_3 WHERE a = 11;
EXECUTE xc_alter_table_update(11, 'bb');
SELECT b FROM xc_alter_table_3 WHERE a = 11;
EXECUTE xc_alter_table_delete(11);
SELECT b FROM xc_alter_table_3 WHERE a = 11 or a = 12;
EXECUTE xc_alter_table_delete(12);
ALTER TABLE xc_alter_table_3 DISTRIBUTE BY ROUNDROBIN;
SELECT count(*), sum(a), avg(a) FROM xc_alter_table_3; -- Check on tuple presence
SELECT * FROM xc_alter_table_3_v;
EXECUTE xc_alter_table_insert(11, 'b');
SELECT b FROM xc_alter_table_3 WHERE a = 11;
EXECUTE xc_alter_table_update(11, 'bb');
SELECT b FROM xc_alter_table_3 WHERE a = 11;
EXECUTE xc_alter_table_delete(11);
SELECT b FROM xc_alter_table_3 WHERE a = 11 or a = 12;
EXECUTE xc_alter_table_delete(12);
ALTER TABLE xc_alter_table_3 DISTRIBUTE BY MODULO(a);
SELECT count(*), sum(a), avg(a) FROM xc_alter_table_3; -- Check on tuple presence
SELECT * FROM xc_alter_table_3_v;
EXECUTE xc_alter_table_insert(11, 'b');
SELECT b FROM xc_alter_table_3 WHERE a = 11;
EXECUTE xc_alter_table_update(11, 'bb');
SELECT b FROM xc_alter_table_3 WHERE a = 11;
EXECUTE xc_alter_table_delete(11);
SELECT b FROM xc_alter_table_3 WHERE a = 11 or a = 12;
EXECUTE xc_alter_table_delete(12);
ALTER TABLE xc_alter_table_3 DISTRIBUTE BY MODULO(b);
SELECT count(*), sum(a), avg(a) FROM xc_alter_table_3; -- Check on tuple presence
SELECT * FROM xc_alter_table_3_v;
EXECUTE xc_alter_table_insert(11, 'b');
SELECT b FROM xc_alter_table_3 WHERE a = 11;
EXECUTE xc_alter_table_update(11, 'bb');
SELECT b FROM xc_alter_table_3 WHERE a = 11;
EXECUTE xc_alter_table_delete(11);
SELECT b FROM xc_alter_table_3 WHERE a = 11 or a = 12;
EXECUTE xc_alter_table_delete(12);
-- Index and redistribution
CREATE INDEX xc_alter_table_3_index ON xc_alter_table_3(a);
ALTER TABLE xc_alter_table_3 DISTRIBUTE BY HASH(a);
SELECT count(*), sum(a), avg(a) FROM xc_alter_table_3; -- Check on tuple presence
SELECT * FROM xc_alter_table_3_v;
EXECUTE xc_alter_table_insert(11, 'b');
SELECT b FROM xc_alter_table_3 WHERE a = 11;
EXECUTE xc_alter_table_update(11, 'bb');
SELECT b FROM xc_alter_table_3 WHERE a = 11;
EXECUTE xc_alter_table_delete(11);
SELECT b FROM xc_alter_table_3 WHERE a = 11 or a = 12;
EXECUTE xc_alter_table_delete(12);
-- Add column on table
ALTER TABLE xc_alter_table_3 ADD COLUMN c int DEFAULT 4;
ALTER TABLE xc_alter_table_3 DISTRIBUTE BY REPLICATION;
SELECT count(*), sum(a), avg(a) FROM xc_alter_table_3;
SELECT * FROM xc_alter_table_3_v;
-- Drop column on table
ALTER TABLE xc_alter_table_3 DROP COLUMN b CASCADE;
ALTER TABLE xc_alter_table_3 DISTRIBUTE BY HASH(a);
SELECT count(*), sum(a), avg(a) FROM xc_alter_table_3;
SELECT * FROM xc_alter_table_3_v;
-- Remanipulate table once again and distribute on old column
ALTER TABLE xc_alter_table_3 DROP COLUMN c;
ALTER TABLE xc_alter_table_3 ADD COLUMN b varchar(3) default 'aaa';
ALTER TABLE xc_alter_table_3 DISTRIBUTE BY HASH(a);
SELECT count(*), sum(a), avg(a) FROM xc_alter_table_3; -- Check on tuple presence
SELECT * FROM xc_alter_table_3_v;
-- Change the node list
SELECT alter_table_change_nodes('xc_alter_table_3', '{1}', 'to', NULL);
SELECT count(*), sum(a), avg(a) FROM xc_alter_table_3; -- Check on tuple presence
SELECT * FROM xc_alter_table_3_v;
-- Add some nodes on it
SELECT alter_table_change_nodes('xc_alter_table_3', '{2,4,5}', 'add', NULL);
SELECT count(*), sum(a), avg(a) FROM xc_alter_table_3; -- Check in tuple presence
SELECT * FROM xc_alter_table_3_v;
-- Remove some nodes on it
SELECT alter_table_change_nodes('xc_alter_table_3', '{3}', 'add', NULL);
SELECT alter_table_change_nodes('xc_alter_table_3', '{2,3,5}', 'delete', NULL);
SELECT count(*), sum(a), avg(a) FROM xc_alter_table_3; -- Check on tuple presence
SELECT * FROM xc_alter_table_3_v;
-- Multiple operations with replication
SELECT alter_table_change_nodes('xc_alter_table_3', '{1,3,4,5}', 'to', 'replication');
SELECT count(*), sum(a), avg(a) FROM xc_alter_table_3; -- Check on tuple presence
SELECT * FROM xc_alter_table_3_v;
-- Manipulate number of nodes to include and remove nodes on a replicated table
-- On removed nodes data is deleted and on new nodes data is added
SELECT alter_table_change_nodes('xc_alter_table_3', '{2,3,5}', 'to', NULL);
SELECT count(*), sum(a), avg(a) FROM xc_alter_table_3; -- Check on tuple presence
SELECT * FROM xc_alter_table_3_v;
-- Re-do a double operation with hash this time
SELECT alter_table_change_nodes('xc_alter_table_3', '{2}', 'delete', 'hash(a)');
SELECT count(*), sum(a), avg(a) FROM xc_alter_table_3; -- Check on tuple presence
SELECT * FROM xc_alter_table_3_v;
-- Error checks
ALTER TABLE xc_alter_table_3 ADD COLUMN b int, DISTRIBUTE BY HASH(a);
-- Clean up
DROP TABLE xc_alter_table_3 CASCADE;
