
CREATE TABLE testr_x14 (a int, b int);
CREATE TABLE testl_x14 (a int, b int);
INSERT INTO testl_x14 SELECT generate_series(1,10000), generate_series(1,10000);
CREATE TABLE testlr_x14 (al int, bl int, ar int, br int);

-- Create a scenario where datanode-datanode connections are established and
-- they access a relation, which is subsequently altered or modified by some
-- other connections. As seen from a bug report, if cache invalidation is not
-- done correctly, the datanode-datanode connections may use stale cached
-- information, leading to many problems

BEGIN;
INSERT INTO testlr_x14 SELECT * FROM testr_x14 RIGHT OUTER JOIN testl_x14 ON testr_x14.a = testl_x14.b;
TRUNCATE testr_x14;
INSERT INTO testr_x14 SELECT * FROM testl_x14;
SELECT count(*) FROM testlr_x14 WHERE al IS NOT NULL;
TRUNCATE testlr_x14;
INSERT INTO testlr_x14 SELECT * FROM testr_x14 RIGHT OUTER JOIN testl_x14 ON testr_x14.a = testl_x14.b;
SELECT count(*) FROM testlr_x14 WHERE al IS NOT NULL;
ROLLBACK;

INSERT INTO testlr_x14 SELECT * FROM testr_x14 RIGHT OUTER JOIN testl_x14 ON testr_x14.a = testl_x14.b;
TRUNCATE testr_x14;
INSERT INTO testr_x14 SELECT * FROM testl_x14;
SELECT count(*) FROM testlr_x14 WHERE al IS NOT NULL;
TRUNCATE testlr_x14;
INSERT INTO testlr_x14 SELECT * FROM testr_x14 RIGHT OUTER JOIN testl_x14 ON testr_x14.a = testl_x14.b;
SELECT count(*) FROM testlr_x14 WHERE al IS NOT NULL;

DROP TABLE testlr_x14, testr_x14, testl_x14;


CREATE TABLE testr_x14 (a int, b int);
CREATE TABLE testl_x14 (a int, b int);
INSERT INTO testl_x14 SELECT generate_series(1,10000), generate_series(1,10000);
CREATE TABLE testlr_x14 (al int, bl int, ar int, br int);

BEGIN;
INSERT INTO testlr_x14 SELECT * FROM testr_x14 RIGHT OUTER JOIN testl_x14 ON testr_x14.a = testl_x14.b;
ALTER TABLE testr_x14 ALTER COLUMN b SET DATA TYPE text;
INSERT INTO testr_x14 SELECT * FROM testl_x14;
SELECT count(*) FROM testlr_x14 WHERE al IS NOT NULL;
TRUNCATE testlr_x14;
INSERT INTO testlr_x14 SELECT testr_x14.a, testr_x14.b::integer, testl_x14.* FROM testr_x14 RIGHT OUTER JOIN testl_x14 ON testr_x14.a = testl_x14.b;
SELECT count(*) FROM testlr_x14 WHERE al IS NOT NULL;
ROLLBACK;

INSERT INTO testlr_x14 SELECT * FROM testr_x14 RIGHT OUTER JOIN testl_x14 ON testr_x14.a = testl_x14.b;
ALTER TABLE testr_x14 ALTER COLUMN b SET DATA TYPE text;
INSERT INTO testr_x14 SELECT * FROM testl_x14;
SELECT count(*) FROM testlr_x14 WHERE al IS NOT NULL;
TRUNCATE testlr_x14;
INSERT INTO testlr_x14 SELECT testr_x14.a, testr_x14.b::integer, testl_x14.* FROM testr_x14 RIGHT OUTER JOIN testl_x14 ON testr_x14.a = testl_x14.b;
SELECT count(*) FROM testlr_x14 WHERE al IS NOT NULL;

DROP TABLE testlr_x14, testr_x14, testl_x14;

