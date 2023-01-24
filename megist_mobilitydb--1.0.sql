-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION megist_mobilitydb" to load this file. \quit

/******************************************************************************
 * Multi Entry R-Tree for tgeompoint using ME-GiST
 ******************************************************************************/

CREATE FUNCTION tpoint_megist_compress(internal)
  RETURNS internal
  AS 'MODULE_PATHNAME', 'Tpoint_megist_compress'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION tpoint_megist_extract(internal, internal, internal)
  RETURNS internal
  AS 'MODULE_PATHNAME', 'Tpoint_megist_extract'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION tpoint_megist_options(internal)
  RETURNS void
  AS 'MODULE_PATHNAME', 'Tpoint_megist_options'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR CLASS megist_tgeompoint_ops
  DEFAULT FOR TYPE tgeompoint USING MEGIST AS
  STORAGE stbox,
  -- overlaps
  OPERATOR  3    && (tgeompoint, tstzspan),
  OPERATOR  3    && (tgeompoint, stbox),
  OPERATOR  3    && (tgeompoint, tgeompoint),
  -- functions
  FUNCTION  1  gist_tgeompoint_consistent(internal, tgeompoint, smallint, oid, internal),
  FUNCTION  2  stbox_gist_union(internal, internal),
  FUNCTION  3  tpoint_megist_compress(internal),
  FUNCTION  5  stbox_gist_penalty(internal, internal, internal),
  FUNCTION  6  stbox_gist_picksplit(internal, internal),
  FUNCTION  7  stbox_gist_same(stbox, stbox, internal),
  FUNCTION  10 tpoint_megist_options(internal),
  FUNCTION  12 tpoint_megist_extract(internal, internal, internal);

/******************************************************************************/