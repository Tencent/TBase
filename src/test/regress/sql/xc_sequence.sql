--
-- XC_SEQUENCE
--

-- Check of callback mechanisms on GTM

-- Sequence DROP and CREATE
-- Rollback a creation
BEGIN;
CREATE SEQUENCE xc_sequence_1;
SELECT nextval('xc_sequence_1'); -- ok
ROLLBACK;
SELECT nextval('xc_sequence_1'); -- fail
-- Commit a creation
BEGIN;
CREATE SEQUENCE xc_sequence_1;
SELECT nextval('xc_sequence_1'); -- ok
COMMIT;
SELECT nextval('xc_sequence_1'); -- ok
-- Rollback a drop
BEGIN;
DROP SEQUENCE xc_sequence_1;
SELECT nextval('xc_sequence_1'); -- fail
ROLLBACK;
SELECT nextval('xc_sequence_1'); -- ok, previous transaction failed
-- Commit a drop
BEGIN;
DROP SEQUENCE xc_sequence_1;
COMMIT;
SELECT nextval('xc_sequence_1'); -- fail

-- SEQUENCE RENAME TO
-- Rollback a renaming
CREATE SEQUENCE xc_sequence_1;
SELECT nextval('xc_sequence_1'); -- ok
BEGIN;
ALTER SEQUENCE xc_sequence_1 RENAME TO xc_sequence_2;
SELECT nextval('xc_sequence_2'); -- ok
ROLLBACK;
SELECT nextval('xc_sequence_1'); -- ok
-- Commit a renaming
BEGIN;
ALTER SEQUENCE xc_sequence_1 RENAME TO xc_sequence_2;
SELECT nextval('xc_sequence_2'); -- ok
COMMIT;
SELECT nextval('xc_sequence_2'); -- ok
DROP SEQUENCE xc_sequence_2;

-- Columns with SERIAL
-- Serial sequence is named xc_sequence_tab1_col2_seq
CREATE TABLE xc_sequence_tab1 (col1 int, col2 serial) DISTRIBUTE BY ROUNDROBIN;
-- Some data
INSERT INTO xc_sequence_tab1 VALUES (1, DEFAULT);
INSERT INTO xc_sequence_tab1 VALUES (2, DEFAULT);
SELECT col1, col2 FROM xc_sequence_tab1 ORDER BY 1;
-- Rollback a SERIAL column drop
BEGIN;
ALTER TABLE xc_sequence_tab1 DROP COLUMN col2;
INSERT INTO xc_sequence_tab1 VALUES (3);
SELECT col1 FROM xc_sequence_tab1 ORDER BY 1;
ROLLBACK;
SELECT nextval('xc_sequence_tab1_col2_seq'); -- ok
-- Commit a SERIAL column drop
BEGIN;
ALTER TABLE xc_sequence_tab1 DROP COLUMN col2;
INSERT INTO xc_sequence_tab1 VALUES (3);
SELECT col1 FROM xc_sequence_tab1 ORDER BY 1;
COMMIT;
DROP TABLE xc_sequence_tab1;
-- Need to recreate here, serial column is no more
CREATE TABLE xc_sequence_tab1 (col1 int, col2 serial) DISTRIBUTE BY ROUNDROBIN;
INSERT INTO xc_sequence_tab1 VALUES (1234, DEFAULT);
SELECT col1, col2 FROM xc_sequence_tab1 ORDER BY 1;
-- Rollback of a table with SERIAL
BEGIN;
DROP TABLE xc_sequence_tab1;
ROLLBACK;
SELECT nextval('xc_sequence_tab1_col2_seq'); -- ok
-- Commit of a table with SERIAL
BEGIN;
DROP TABLE xc_sequence_tab1;
COMMIT;
-- Recreate a sequence with the same name as previous SERIAL one
CREATE SEQUENCE xc_sequence_tab1_col2_seq START 2344;
SELECT nextval('xc_sequence_tab1_col2_seq'); -- ok
DROP SEQUENCE xc_sequence_tab1_col2_seq;

-- As simple test that sequences are dropped properly
CREATE SEQUENCE xl_s1;
SELECT nextval('xl_s1');
DROP SEQUENCE xl_s1;

-- Check if rollback reverses sequence drop properly
CREATE SEQUENCE xl_s1;
BEGIN;
DROP SEQUENCE xl_s1;
ROLLBACK;
DROP SEQUENCE xl_s1;

-- Check if commit makes sequence drop permanent
CREATE SEQUENCE xl_s1;
BEGIN;
DROP SEQUENCE xl_s1;
COMMIT;
DROP SEQUENCE xl_s1; -- error

-- Drop a sequence and recreate another sequence with the same name in the same
-- transaction. Check that transaction abort does not cause any surprising
-- behaviour
CREATE SEQUENCE xl_s1;
SELECT nextval('xl_s1');
SELECT nextval('xl_s1');
BEGIN;
DROP SEQUENCE xl_s1;
CREATE SEQUENCE xl_s1;	-- create again with the same name
SELECT nextval('xl_s1');-- new value
ROLLBACK;
SELECT nextval('xl_s1');-- sequence value changes are not transactional
DROP SEQUENCE xl_s1;

-- Check sequence renaming works ok
CREATE SEQUENCE xl_s1;
SELECT nextval('xl_s1');
SELECT nextval('xl_s1');
ALTER SEQUENCE xl_s1 RENAME TO xl_s1_newname;
SELECT nextval('xl_s1');	-- error
SELECT nextval('xl_s1_newname');	-- continue with the previous value-space
DROP SEQUENCE xl_s1;	-- error
DROP SEQUENCE xl_s1_newname;	-- should be ok

-- A combination of ALTER and RENAME should work ok when transaction aborts
CREATE SEQUENCE xl_s1;
SELECT nextval('xl_s1');
SELECT nextval('xl_s1');
BEGIN;
ALTER SEQUENCE xl_s1 RESTART;
SELECT nextval('xl_s1');	-- restart value
DROP SEQUENCE xl_s1;
ROLLBACK;
SELECT nextval('xl_s1');
DROP SEQUENCE xl_s1;

-- A combination of ALTER and RENAME should work ok when transacion commits
CREATE SEQUENCE xl_s1;
SELECT nextval('xl_s1');
SELECT nextval('xl_s1');
BEGIN;
ALTER SEQUENCE xl_s1 RESTART;
SELECT nextval('xl_s1');	-- restart value
ALTER SEQUENCE xl_s1 RENAME TO xl_s1_newname;
SELECT nextval('xl_s1_newname');
ALTER SEQUENCE xl_s1_newname RENAME TO xl_s1;
ALTER SEQUENCE xl_s1 RESTART;
COMMIT;
SELECT nextval('xl_s1');
DROP SEQUENCE xl_s1;


-- Multiple RENAMEs in the same transaction
CREATE SEQUENCE xl_s1;
SELECT nextval('xl_s1');
SELECT nextval('xl_s1');
BEGIN;
ALTER SEQUENCE xl_s1 RESTART;
SELECT nextval('xl_s1');	-- restart value
ALTER SEQUENCE xl_s1 RENAME TO xl_s1_newname;
SELECT nextval('xl_s1_newname');
ALTER SEQUENCE xl_s1_newname RENAME TO xl_s1;
ALTER SEQUENCE xl_s1 RESTART;
DROP SEQUENCE xl_s1;
COMMIT;
SELECT nextval('xl_s1');
DROP SEQUENCE xl_s1;

-- Multiple RENAMEs in the same transaction and the transaction aborts
CREATE SEQUENCE xl_s1;
SELECT nextval('xl_s1');
SELECT nextval('xl_s1');
BEGIN;
ALTER SEQUENCE xl_s1 RESTART;
SELECT nextval('xl_s1');	-- restart value
ALTER SEQUENCE xl_s1 RENAME TO xl_s1_newname;
SELECT nextval('xl_s1_newname');
ALTER SEQUENCE xl_s1_newname RENAME TO xl_s1;
ALTER SEQUENCE xl_s1 RESTART;
DROP SEQUENCE xl_s1;
ROLLBACK;
SELECT nextval('xl_s1');
DROP SEQUENCE xl_s1;

-- Rename back to original value, but abort the transaction
CREATE SEQUENCE xl_s1;
SELECT nextval('xl_s1');
BEGIN;
ALTER SEQUENCE xl_s1 RENAME TO xl_s1_newname;
ALTER SEQUENCE xl_s1_newname RENAME TO xl_s1;
SELECT nextval('xl_s1');
ROLLBACK;
SELECT nextval('xl_s1');
DROP SEQUENCE xl_s1;

-- Rename back to original value
CREATE SEQUENCE xl_s1;
SELECT nextval('xl_s1');
BEGIN;
ALTER SEQUENCE xl_s1 RENAME TO xl_s1_newname;
ALTER SEQUENCE xl_s1_newname RENAME TO xl_s1;
SELECT nextval('xl_s1');
COMMIT;
SELECT nextval('xl_s1');

CREATE TABLE xl_testtab (a serial, b int);
ALTER TABLE xl_testtab RENAME TO xl_testtab_newname;
\d+ xl_testtab_newname

