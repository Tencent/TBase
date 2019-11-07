CREATE EXTENSION tsm_system_time;
CREATE TABLE test_tablesample (id int, name text);
INSERT INTO test_tablesample SELECT i, repeat(i::text, 1000)
  FROM generate_series(0, 30) s(i);

EXPLAIN (COSTS OFF)
SELECT * FROM
  (VALUES (0),(100000)) v(time),
  LATERAL (SELECT COUNT(*) FROM test_tablesample
           TABLESAMPLE system_time (time)) ss;

SELECT * FROM
  (VALUES (0),(100000)) v(time),
  LATERAL (SELECT COUNT(*) FROM test_tablesample
           TABLESAMPLE system_time (time)) ss;

DROP TABLE test_tablesample;
DROP EXTENSION tsm_system_time;  -- fail, view depends on extension
