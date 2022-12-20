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

#define NORMALIZE       true
#define NORMALIZE_NO    false

#define PG_GETARG_TEMPORAL_P(X)    ((Temporal *) PG_GETARG_VARLENA_P(X))

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

/*****************************************************************************
 * ME-GiST extract methods
 *****************************************************************************/

static STBOX **
tinstant_split(TInstant *inst, int32 *count)
{
  STBOX **result = palloc(sizeof(STBOX *));
  result[0] = palloc(sizeof(STBOX));
  tinstant_set_bbox(inst, result[0]);
  *count = 1;
  return result;
}

static STBOX **
tsequence_split(TSequence *seq, int32 *count)
{
  STBOX **result;
  STBOX box1;
  int segs_per_split, segs_this_split, k;

  if (seq->count <= 1)
  {
    result = palloc(sizeof(STBOX *));
    result[0] = palloc(sizeof(STBOX));
    tsequence_set_bbox(seq, result[0]);
    *count = 1;
    return result;
  }

  segs_per_split = ceil((double) (seq->count - 1) / (double) (*count));
  if (ceil((double) (seq->count - 1) / (double) segs_per_split) < *count)
    *count = ceil((double) (seq->count - 1) / (double) segs_per_split);

  k = 0;
  result = palloc(sizeof(STBOX *) * (*count));
  for (int i = 0; i < seq->count - 1; i += segs_per_split)
  {
    segs_this_split = segs_per_split;
    if (seq->count - 1 - i < segs_per_split)
      segs_this_split = seq->count - 1 - i;
    result[k] = palloc(sizeof(STBOX));
    tinstant_set_bbox(tsequence_inst_n(seq, i), result[k]);
    for (int j = 1; j < segs_this_split + 1; j++)
    {
      tinstant_set_bbox(tsequence_inst_n(seq, i + j), &box1);
      stbox_expand(&box1, result[k]);
    }
    k++;
  }
  return result;
}

static STBOX **
tsequenceset_split(TSequenceSet *ss, int32 *count)
{
  STBOX **result = palloc(sizeof(STBOX *));
  result[0] = palloc(sizeof(STBOX));
  tsequenceset_set_bbox(ss, result[0]);
  *count = 1;
  return result;
}

static STBOX **
tpoint_split(Temporal *temp, int32 *count)
{
  STBOX **result;
  ensure_valid_tempsubtype(temp->subtype);
  if (temp->subtype == TINSTANT)
    result = tinstant_split((TInstant *) temp, count);
  else if (temp->subtype == TSEQUENCE)
    result = tsequence_split((TSequence *) temp, count);
  else // temp->subtype == TSEQUENCESET
    result = tsequenceset_split((TSequenceSet *) temp, count);
  return result;
}

PG_FUNCTION_INFO_V1(Tpoint_megist_extract);
/**
 * ME-GiST extract methods for temporal points
 */
PGDLLEXPORT Datum
Tpoint_megist_extract(PG_FUNCTION_ARGS)
{
  Temporal *temp  = PG_GETARG_TEMPORAL_P(0);
  int32    *nkeys = (int32 *) PG_GETARG_POINTER(1);
  // bool   **nullFlags = (bool **) PG_GETARG_POINTER(2);

  STBOX **boxes;
  Datum *keys;
  int i;

  *nkeys = 20;
  boxes = tpoint_split(temp, nkeys);
  keys = palloc(sizeof(Datum) * (*nkeys));
  for (i = 0; i < *nkeys; ++i)
  {
    keys[i] = PointerGetDatum(boxes[i]);
  }
  PG_RETURN_POINTER(keys);
}

/*****************************************************************************/