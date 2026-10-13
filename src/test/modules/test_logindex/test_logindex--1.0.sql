/* src/test/modules/test_logindex/test_logindex--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_logindex" to load this file. \quit

CREATE FUNCTION test_bitpos()
RETURNS int4 STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_ringbuf()
RETURNS int4 STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_mini_trans()
RETURNS int4 STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_logindex()
RETURNS int4 STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_fullpage()
RETURNS int4 STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_fullpage_roll_table()
RETURNS int8 STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_polar_rel_size_cache()
RETURNS int4 STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_checkpoint_ringbuf()
RETURNS int4 STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_get_polar_parallel_replay_mode()
RETURNS text
AS 'MODULE_PATHNAME', 'test_get_polar_parallel_replay_mode'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION test_fullpage_max_no() RETURNS bigint
AS 'MODULE_PATHNAME', 'test_fullpage_max_no'
LANGUAGE C STRICT;

CREATE FUNCTION test_fullpage_resume(bigint) RETURNS bigint
AS 'MODULE_PATHNAME', 'test_fullpage_resume'
LANGUAGE C STRICT;

CREATE FUNCTION test_fullpage_replay_counter() RETURNS int
AS 'MODULE_PATHNAME', 'test_fullpage_replay_counter'
LANGUAGE C STRICT;

CREATE FUNCTION test_fullpage_sweep() RETURNS int
AS 'MODULE_PATHNAME', 'test_fullpage_sweep'
LANGUAGE C STRICT;

CREATE FUNCTION test_fullpage_publish_inflight(bigint) RETURNS int
AS 'MODULE_PATHNAME', 'test_fullpage_publish_inflight'
LANGUAGE C STRICT;

CREATE FUNCTION test_fullpage_inflight_occupied() RETURNS int
AS 'MODULE_PATHNAME', 'test_fullpage_inflight_occupied'
LANGUAGE C STRICT;
