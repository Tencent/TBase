-- #93
-- SQLsmith: ERROR: invalid regexp option:

select
          subq_28.c0 as c0,
          subq_29.c0 as c1
        from
          (select
                  83 as c0,
                  sample_76.nodeoids as c1,
                  7 as c2
                from
                  pg_catalog.pgxc_class as sample_76 tablesample system (1)
                where (select character_maximum_length from information_schema.domains limit 1 offset 36)
         < sample_76.pchashbuckets) as subq_28
            right join (select
                  sample_77.objoid as c0,
                  sample_77.objoid as c1,
                  52 as c2,
                  pg_catalog.random() as c3,
                  sample_77.description as c4,
                  sample_77.objsubid as c5,
                  53 as c6,
                  73 as c7,
                  sample_77.objsubid as c8
                from
                  pg_catalog.pg_description as sample_77 tablesample bernoulli (4.5)
                where sample_77.objoid <> sample_77.classoid
                limit 174) as subq_29
            on (subq_28.c0 = subq_29.c2 )
        where (53 = pg_catalog.char_length(
              cast(pg_catalog.regexp_replace(
                cast(subq_29.c4 as text),


                cast(subq_29.c4 as text),


                cast(subq_29.c4 as text),


                cast(pg_catalog.timeofday() as text)) as text)))
          and ((subq_29.c7 is NULL)
            or ((subq_28.c2 <= 68)
              or ((select character_maximum_length from information_schema.domains limit 1 offset 39)
         <= 100)))
        limit 94;

-- #91
-- SQLsmith: ERROR: int2_accum_inv called with NULL state

SET sequence_range = 1;
CREATE SEQUENCE xl_INSERT_SEQ;

CREATE TABLE xl_funct (
        a integer,
        b INT DEFAULT nextval('xl_insert_seq')
) DISTRIBUTE BY HASH (a);

INSERT INTO xl_funct (a) VALUES (1);
INSERT INTO xl_funct (a) VALUES (2);
INSERT INTO xl_funct (a) VALUES (3);
SELECT * FROM xl_funct;

CREATE TABLE xl_join_t1 (val1 int, val2 int);
CREATE TABLE xl_join_t2 (val1 int, val2 int);
CREATE TABLE xl_join_t3 (val1 int, val2 int);
INSERT INTO xl_join_t1 VALUES (1,10),(2,20);
INSERT INTO xl_join_t2 VALUES (3,30),(4,40);
INSERT INTO xl_join_t3 VALUES (5,50),(6,60);EXPLAIN SELECT * FROM xl_join_t1
        INNER JOIN xl_join_t2 ON xl_join_t1.val1 = xl_join_t2.val2
        INNER JOIN xl_join_t3 ON xl_join_t1.val1 = xl_join_t3.val1;
SELECT * FROM xl_join_t1
        INNER JOIN xl_join_t2 ON xl_join_t1.val1 = xl_join_t2.val2
        INNER JOIN xl_join_t3 ON xl_join_t1.val1 = xl_join_t3.val1;

select
          subq_3.c1 as c0,
          subq_9.c0 as c1
        from
          (select
                  subq_2.c1 as c0,
                  50 as c1
                from
                  pg_catalog.pg_pltemplate as sample_2 tablesample system (4.6) ,
                  lateral (select
                        sample_2.tmpldbacreate as c0,
                        pg_catalog.pg_advisory_unlock_all() as c1,
                        sample_2.tmplvalidator as c2
                      from
                        public.xl_join_t3 as sample_3 tablesample bernoulli (9.2)
                      where sample_3.val1 <> pg_catalog.txid_current()
                      limit 55) as subq_2
                where 92 >= pg_catalog.pg_event_trigger_table_rewrite_reason()
                limit 60) as subq_3
            inner join (select
                    sample_10.b as c0
                  from
                    public.xl_funct as sample_10 tablesample system (1.8)
                  where sample_10.b < sample_10.a
                  limit 42) as subq_9
              right join (select
                    41 as c0,
                    sample_11.rngsubdiff as c1
                  from
                    pg_catalog.pg_range as sample_11 tablesample system (9.4)
                  where (pg_catalog.numeric_accum_inv(
                        cast(null as numeric_agg_state),


                        cast(null as numeric)) is NULL)
                    and (sample_11.rngsubopc <> sample_11.rngsubopc)
                  limit 130) as subq_10
              on (subq_9.c0 = subq_10.c0 )
            on (subq_3.c1 = subq_10.c0 )
        where subq_9.c0 <> subq_9.c0;
DROP TABLE xl_join_t1;
DROP TABLE xl_join_t2;
DROP TABLE xl_join_t3;
DROP SEQUENCE xl_INSERT_SEQ cascade;
DROP TABLE xl_funct;

-- #88
-- SQLsmith: Error: unsupported data type for HASH locator:

CREATE TABLE xl_join_t1 (val1 int, val2 int);
CREATE TABLE xl_join_t2 (val1 int, val2 int);
CREATE TABLE xl_join_t3 (val1 int, val2 int);
INSERT INTO xl_join_t1 VALUES (1,10),(2,20);
INSERT INTO xl_join_t2 VALUES (3,30),(4,40);
INSERT INTO xl_join_t3 VALUES (5,50),(6,60);EXPLAIN SELECT * FROM xl_join_t1
        INNER JOIN xl_join_t2 ON xl_join_t1.val1 = xl_join_t2.val2
        INNER JOIN xl_join_t3 ON xl_join_t1.val1 = xl_join_t3.val1;
SELECT * FROM xl_join_t1
        INNER JOIN xl_join_t2 ON xl_join_t1.val1 = xl_join_t2.val2
        INNER JOIN xl_join_t3 ON xl_join_t1.val1 = xl_join_t3.val1;


insert into public.xl_join_t1 values (
        (select character_maximum_length from information_schema.routines limit 1 offset 15)
        ,
        33);

DROP TABLE xl_join_t1;
DROP TABLE xl_join_t2;
DROP TABLE xl_join_t3;

-- #87
-- SQLsmith error: cache lookup failed for function 0

CREATE TABLE xl_join_t1 (val1 int, val2 int);
CREATE TABLE xl_join_t2 (val1 int, val2 int);
CREATE TABLE xl_join_t3 (val1 int, val2 int);
INSERT INTO xl_join_t1 VALUES (1,10),(2,20);
INSERT INTO xl_join_t2 VALUES (3,30),(4,40);
INSERT INTO xl_join_t3 VALUES (5,50),(6,60);
EXPLAIN SELECT * FROM xl_join_t1
        INNER JOIN xl_join_t2 ON xl_join_t1.val1 = xl_join_t2.val2
        INNER JOIN xl_join_t3 ON xl_join_t1.val1 = xl_join_t3.val1;
SELECT * FROM xl_join_t1
        INNER JOIN xl_join_t2 ON xl_join_t1.val1 = xl_join_t2.val2
        INNER JOIN xl_join_t3 ON xl_join_t1.val1 = xl_join_t3.val1;

select
          55 as c0,
          subq_3.c0 as c1,
          subq_3.c1 as c2
        from
          (select
                  subq_0.c4 as c0,
                  subq_0.c3 as c1,
                  subq_0.c1 as c2,
                  pg_catalog.pg_backend_pid() as c3,
                  39 as c4,
                  50 as c5,
                  (select conforencoding from pg_catalog.pg_conversion limit 1 offset 8)
         as c6
                from
                  (select
                        81 as c0,
                        sample_1.val2 as c1,
                        (select ordinal_position from information_schema.columns limit 1 offset 29)
         as c2,
                        sample_1.val1 as c3,
                        sample_1.val2 as c4
                      from
                        public.xl_join_t3 as sample_1 tablesample system (5.5)
                      where true) as subq_0,
                  lateral (select
                        75 as c0
                      from
                        information_schema.routines as ref_1,
                        lateral (select
                              32 as c0
                            from
                              pg_catalog.pg_database as sample_2 tablesample system (9.1)
                            where ((select pid from pg_catalog.pg_stat_activity limit 1 offset 1)
         > (select ordinal_position from information_schema.attributes limit 1 offset 36)
        )
                              and (subq_0.c0 <> subq_0.c1)
                            limit 64) as subq_1
                      where pg_catalog.pg_get_function_identity_arguments(
                          cast(pg_catalog.pg_my_temp_schema() as oid)) is NULL) as subq_2
                where ((select pid from pg_catalog.pg_stat_activity limit 1 offset 22)
         <= 50)
                  or ((72 <= subq_0.c2)
                    and (((98 >= subq_0.c0)
                        and (subq_2.c0 <> (select character_maximum_length from information_schema.element_types limit 1 offset 30)
        ))
                      and (EXISTS (
                        select
                            ref_2.collation_name as c0,
                            ref_2.scope_schema as c1,
                            47 as c2,
                            pg_catalog.pgxc_pool_check() as c3,
                            76 as c4,
                            83 as c5,
                            pg_catalog.current_user() as c6
                          from
                            information_schema.element_types as ref_2
                          where pg_catalog.time_pl_interval(
                              cast(pg_catalog.time(
                                cast(null as timestamp without time zone)) as time without time zone),


                              cast(null as interval)) is NULL
                          limit 129))))) as subq_3
            inner join (select
                  sample_3.classoid as c0,
                  23 as c1,
                  pg_catalog.current_user() as c2,
                  cast(coalesce((select ordinal_position from information_schema.parameters limit 1 offset 5)
        ,

                    95) as integer) as c3
                from
                  pg_catalog.pg_description as sample_3 tablesample system (9.1)
                where sample_3.description !~ sample_3.description
                limit 46) as subq_4
            on (subq_3.c5 = subq_4.c1 ),
          lateral (select
                pg_catalog.pg_last_xlog_receive_location() as c0,
                sample_4.setrole as c1,
                pg_catalog.pg_stat_get_buf_alloc() as c2
              from
                pg_catalog.pg_db_role_setting as sample_4 tablesample bernoulli (1.8)
              where subq_3.c5 > (select character_maximum_length from information_schema.element_types limit 1 offset 16)

              limit 120) as subq_5
        where true
        limit 87;

DROP TABLE xl_join_t1;
DROP TABLE xl_join_t2;
DROP TABLE xl_join_t3;



-- #102 Issue
CREATE TABLE xl_join_t1 (val1 int, val2 int);
CREATE TABLE xl_join_t2 (val1 int, val2 int);
CREATE TABLE xl_join_t3 (val1 int, val2 int);
INSERT INTO xl_join_t1 VALUES (1,10),(2,20);
INSERT INTO xl_join_t2 VALUES (3,30),(4,40);
INSERT INTO xl_join_t3 VALUES (5,50),(6,60);
EXPLAIN SELECT * FROM xl_join_t1
        INNER JOIN xl_join_t2 ON xl_join_t1.val1 = xl_join_t2.val2
        INNER JOIN xl_join_t3 ON xl_join_t1.val1 = xl_join_t3.val1;
SELECT * FROM xl_join_t1
        INNER JOIN xl_join_t2 ON xl_join_t1.val1 = xl_join_t2.val2
        INNER JOIN xl_join_t3 ON xl_join_t1.val1 = xl_join_t3.val1;

select
  cast(coalesce(8,
    (select pid from pg_catalog.pg_stat_activity limit 1 offset 29)
) as integer) as c0,
  33 as c1,
  (select objsubid from pg_catalog.pg_seclabel limit 1 offset 34)
 as c2,
  (select ordinal_position from information_schema.parameters limit 1 offset 1)
 as c3,
  cast(coalesce(pg_catalog.pg_postmaster_start_time(),
      pg_catalog.min(
        cast(null as timestamp with time zone)) over (partition by subq_1.c1 order by subq_1.c1,subq_1.c5)) as timestamp with time zone) as c4,
  (select character_maximum_length from information_schema.domains limit 1 offset 36)
 as c5
from
  (select
        (select character_maximum_length from information_schema.user_defined_types limit 1 offset 6)
 as c0,
        sample_7.conpfeqop as c1,
        sample_6.amprocfamily as c2,
          pg_catalog.string_agg(
            cast(sample_7.consrc as text),
            cast(sample_7.consrc as text)) over (partition by sample_6.amprocrighttype order by sample_6.amprocfamily,sample_6.amprocrighttype) as c3,
        34 as c4,
        pg_catalog.abs(
          cast(cast(coalesce((select total_time from pg_catalog.pg_stat_user_functions limit 1 offset 6)
,
            null) as double precision) as double precision)) as c5
      from
        pg_catalog.pg_amop as ref_11
            left join pg_catalog.pg_amproc as sample_6 tablesample system (7)
            on (ref_11.amopmethod = sample_6.amprocfamily )
          inner join pg_catalog.pg_constraint as sample_7 tablesample system (9.5)
          on (ref_11.amopsortfamily = sample_7.connamespace ),
        lateral (select
              ref_11.amopmethod as c0
            from
              pg_catalog.pg_user_mapping as ref_12
            where ((sample_7.confdeltype is NULL)
                and (((select character_maximum_length from information_schema.element_types limit 1 offset 16)
 >= 39)
                  and (99 > (select objsubid from pg_catalog.pg_depend limit 1 offset 10)
)))
              or (59 <> 82)
            limit 75) as subq_0
      where EXISTS (
        select
            76 as c0,
            ref_13.val2 as c1,
            7 as c2,
            pg_catalog.has_server_privilege(
              cast(null as oid),
              cast(pg_catalog.to_hex(
                cast(null as bigint)) as text),
              cast(null as text)) as c3,
              pg_catalog.bit_out(
              cast(null as bit)) as c4,
              pg_catalog.int82ge(
              cast(pg_catalog.pg_stat_get_db_conflict_snapshot(
                cast(pg_catalog.pg_my_temp_schema() as oid)) as bigint),
              cast(null as smallint)) as c5,
              ref_13.val1 as c6
          from
              public.xl_join_t3 as ref_13
          where (ref_13.val2 = pg_catalog.pg_stat_get_xact_tuples_inserted(
                cast(pg_catalog.lo_import(
                  cast((select name from pg_catalog.pg_cursors limit 1 offset 21)
 as text)) as oid)))
              or (25 > (select ordinal_position from information_schema.attributes limit 1 offset 17)
))
      limit 87) as subq_1
where
cast(coalesce((select node_port from pg_catalog.pgxc_node limit 1 offset 9)
,
    cast(coalesce(pg_catalog.pg_backend_pid(),
      subq_1.c0) as integer)) as integer) is NULL;


DROP TABLE xl_join_t1;
DROP TABLE xl_join_t2;
DROP TABLE xl_join_t3;


