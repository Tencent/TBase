CREATE FUNCTION storm_database_stats(
    OUT datname text,
    OUT conn_cnt int8,
    OUT select_cnt int8,
    OUT insert_cnt int8,
    OUT update_cnt int8,
    OUT delete_cnt int8,
    OUT ddl_cnt int8
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Register a view on the function for ease of use.
CREATE VIEW storm_database_stats AS
  SELECT * FROM storm_database_stats();

