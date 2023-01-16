/*
 * megist_mobilitydb.C 
 *
 * Multi Entry R-Tree for tgeompoint using ME-GiST
 *
 * Author: Maxime Schoemans <maxime.schoemans@ulb.be>
 */

#include "postgres.h"
#include "fmgr.h"
#include "access/gist.h"
#include "access/reloptions.h"
#include "utils/timestamp.h"
#include "utils/datetime.h"
#include "utils/date.h"

#include <meos.h>
#include <meos_internal.h>

PG_MODULE_MAGIC;

#define ANYTEMPSUBTYPE  0
#define TINSTANT        1
#define TSEQUENCE       2
#define TSEQUENCESET    3

#define PG_GETARG_TEMPORAL_P(X)    ((Temporal *) PG_GETARG_VARLENA_P(X))

/* number boxes for extract function */
#define MEGIST_EXTRACT_BOXES_DEFAULT    10
#define MEGIST_EXTRACT_BOXES_MAX        1000
#define MEGIST_EXTRACT_GET_BOXES()   (PG_HAS_OPCLASS_OPTIONS() ? \
          ((MEGISTOptions *) PG_GET_OPCLASS_OPTIONS())->num_boxes : \
          MEGIST_EXTRACT_BOXES_DEFAULT)

/* gist_int_ops opclass options */
typedef struct
{
  int32   vl_len_;    /* varlena header (do not touch directly!) */
  int     num_boxes;   /* number of ranges */
} MEGISTOptions;

/*****************************************************************************
 * ME-GiST extract methods
 *****************************************************************************/

PG_FUNCTION_INFO_V1(Tpoint_megist_compress);
/**
 * ME-GiST compress methods for temporal points
 */
PGDLLEXPORT Datum
Tpoint_megist_compress(PG_FUNCTION_ARGS)
{
  GISTENTRY *entry = (GISTENTRY *) PG_GETARG_POINTER(0);
  PG_RETURN_POINTER(entry);
}

PG_FUNCTION_INFO_V1(Tpoint_megist_options);
/**
 * ME-GiST options for temporal points
 */
PGDLLEXPORT Datum
Tpoint_megist_options(PG_FUNCTION_ARGS)
{
  local_relopts *relopts = (local_relopts *) PG_GETARG_POINTER(0);

  init_local_reloptions(relopts, sizeof(MEGISTOptions));
  add_local_int_reloption(relopts, "k",
              "number of boxes for extract method",
              MEGIST_EXTRACT_BOXES_DEFAULT, 1, MEGIST_EXTRACT_BOXES_MAX,
              offsetof(MEGISTOptions, num_boxes));

  PG_RETURN_VOID();
}

/*****************************************************************************
 * ME-GiST extract methods
 *****************************************************************************/

static STBox **
tinstant_extract1(const TInstant *inst, int32 *nkeys)
{
  STBox **result = palloc(sizeof(STBox *));
  result[0] = palloc(sizeof(STBox));
  tinstant_set_bbox(inst, result[0]);
  *nkeys = 1;
  return result;
}

static STBox **
tsequence_extract1(const TSequence *seq, int32 *nkeys)
{
  STBox **result = palloc(sizeof(STBox *));
  result[0] = palloc(sizeof(STBox));
  tsequence_set_bbox(seq, result[0]);
  *nkeys = 1;
  return result;
}

static STBox **
tsequenceset_extract1(const TSequenceSet *ss, int32 *nkeys)
{
  STBox **result = palloc(sizeof(STBox *));
  result[0] = palloc(sizeof(STBox));
  tsequenceset_set_bbox(ss, result[0]);
  *nkeys = 1;
  return result;
}

static STBox **
tpoint_extract(FunctionCallInfo fcinfo, const Temporal *temp, 
  STBox ** (*tsequence_extract)(FunctionCallInfo fcinfo, 
    const TSequence *, int32 *), int32 *nkeys)
{
  STBox **result;
  if (temp->subtype == TINSTANT)
    result = tinstant_extract1((TInstant *) temp, nkeys);
  else if (temp->subtype == TSEQUENCE)
  {
    const TSequence *seq = (TSequence *) temp;
    if (seq->count <= 1)
      result = tsequence_extract1(seq, nkeys);
    else
      result = tsequence_extract(fcinfo, seq, nkeys);
  }
  else if (temp->subtype == TSEQUENCESET)
    result = tsequenceset_extract1((TSequenceSet *) temp, nkeys);
  else
    elog(ERROR, "unknown subtype for temporal type: %d", temp->subtype);
  return result;
}

static Datum
tpoint_megist_extract(FunctionCallInfo fcinfo, 
  STBox ** (*tsequence_extract)(FunctionCallInfo fcinfo, 
    const TSequence *, int32 *))
{
  Temporal *temp  = PG_GETARG_TEMPORAL_P(0);
  int32    *nkeys = (int32 *) PG_GETARG_POINTER(1);
  // bool   **nullFlags = (bool **) PG_GETARG_POINTER(2);

  STBox **boxes = tpoint_extract(fcinfo, temp, tsequence_extract, nkeys);
  Datum *keys = palloc(sizeof(Datum) * (*nkeys));
  for (int i = 0; i < *nkeys; ++i)
  {
    keys[i] = PointerGetDatum(boxes[i]);
  }
  PG_RETURN_POINTER(keys);
}

/*****************************************************************************/

/* Equisplit */

static STBox **
tsequence_equisplit(FunctionCallInfo fcinfo, const TSequence *seq, int32 *nkeys)
{
  STBox **result;
  STBox box1;
  int segs_per_split, segs_this_split, k;
  int32 count = MEGIST_EXTRACT_GET_BOXES();

  segs_per_split = ceil((double) (seq->count - 1) / (double) (count));
  if (ceil((double) (seq->count - 1) / (double) segs_per_split) < count)
    count = ceil((double) (seq->count - 1) / (double) segs_per_split);

  k = 0;
  result = palloc(sizeof(STBox *) * (count));
  for (int i = 0; i < seq->count - 1; i += segs_per_split)
  {
    segs_this_split = segs_per_split;
    if (seq->count - 1 - i < segs_per_split)
      segs_this_split = seq->count - 1 - i;
    result[k] = palloc(sizeof(STBox));
    tinstant_set_bbox(tsequence_inst_n(seq, i), result[k]);
    for (int j = 1; j < segs_this_split + 1; j++)
    {
      tinstant_set_bbox(tsequence_inst_n(seq, i + j), &box1);
      stbox_expand(&box1, result[k]);
    }
    k++;
  }
  *nkeys = count;
  return result;
}

PG_FUNCTION_INFO_V1(Tpoint_megist_equisplit);
/**
 * ME-GiST extract methods for temporal points
 */
PGDLLEXPORT Datum
Tpoint_megist_equisplit(PG_FUNCTION_ARGS)
{
  return tpoint_megist_extract(fcinfo, &tsequence_equisplit);
}

/*****************************************************************************/