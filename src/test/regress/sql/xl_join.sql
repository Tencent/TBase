
CREATE TABLE xl_join_t1 (val1 int, val2 int);
CREATE TABLE xl_join_t2 (val1 int, val2 int);
CREATE TABLE xl_join_t3 (val1 int, val2 int);

INSERT INTO xl_join_t1 VALUES (1,10),(2,20);
INSERT INTO xl_join_t2 VALUES (3,30),(4,40);
INSERT INTO xl_join_t3 VALUES (5,50),(6,60);

EXPLAIN (COSTS OFF)
SELECT * FROM xl_join_t1
	INNER JOIN xl_join_t2 ON xl_join_t1.val1 = xl_join_t2.val2 
	INNER JOIN xl_join_t3 ON xl_join_t1.val1 = xl_join_t3.val1;

SELECT * FROM xl_join_t1
	INNER JOIN xl_join_t2 ON xl_join_t1.val1 = xl_join_t2.val2 
	INNER JOIN xl_join_t3 ON xl_join_t1.val1 = xl_join_t3.val1;

DROP TABLE xl_join_t1;
DROP TABLE xl_join_t2;
DROP TABLE xl_join_t3;
