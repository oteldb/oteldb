package chstorage

import (
	"time"

	"github.com/oteldb/oteldb/internal/chstorage/chsql"
)

// stepFanoutExpr is the per-row "step_ms_val" expression shared by every
// step-bucketing offload query (PromQL rate/increase/delta offload, LogQL
// sample aggregation offload). It enumerates every output step (epoch
// milliseconds) whose window (step-window, step] covers the row, given the
// CTEs defined by [applyStepFanoutCTEs]: first_step_ms, last_step_ms,
// step_ms, window_ms, point_offset_ms, first_sample_step_ms, num_steps.
//
// A row whose window spans multiple steps (i.e. step < window) produces
// multiple output rows once arrayJoin-ed — see the diagram in
// applyStepFanoutCTEs.
var stepFanoutExpr = func() chsql.Expr {
	var (
		firstStepMSIdent     = chsql.Ident("first_step_ms")
		lastStepMSIdent      = chsql.Ident("last_step_ms")
		stepMSIdent          = chsql.Ident("step_ms")
		windowMSIdent        = chsql.Ident("window_ms")
		pointOffsetMSIdent   = chsql.Ident("point_offset_ms")
		firstSampleStepIdent = chsql.Ident("first_sample_step_ms")
		numStepsIdent        = chsql.Ident("num_steps")
	)

	mapLambda := chsql.Lambda([]string{"i"},
		chsql.Add(firstSampleStepIdent, chsql.Mul(chsql.ColumnToInt64("i"), stepMSIdent)),
	)
	rangeExpr := chsql.Range(numStepsIdent)

	filterLambda := chsql.Lambda([]string{"s"}, chsql.JoinAnd(
		chsql.Gte(chsql.Ident("s"), firstStepMSIdent),
		chsql.Lte(chsql.Ident("s"), lastStepMSIdent),
		chsql.Lt(chsql.Ident("s"), chsql.Add(pointOffsetMSIdent, windowMSIdent)),
	))

	return chsql.ArrayJoin(
		chsql.ArrayFilter(
			filterLambda,
			chsql.ArrayMap(mapLambda, rangeExpr),
		),
	)
}()

// applyStepFanoutCTEs adds the WITH clauses that [stepFanoutExpr] depends on:
// for each row's timeColumn, compute which output steps in [start, end]
// (spaced step apart, each covering a window-wide range ending at the step,
// optionally shifted back by offset) the row's timestamp falls into.
//
// Each step S covers the range (S-window, S] (adjusted for offset). A single
// row near the boundary of two windows belongs to both, so one input row may
// produce multiple output rows once stepFanoutExpr is selected:
//
//	time ──────────────────────────────────────────────────────►
//
//	steps     │   S1    │   S2    │   S3    │   S4    │
//	          ├─────────┼─────────┼─────────┼─────────┤
//	S1 window [◄──────window──────]
//	S2 window          [◄──────window──────]
//	S3 window                    [◄──────window──────]
//
//	row A ─────────────────────┼  (falls in S2 and S3)
//	row B ──────────┼            (falls in S1 and S2)
//
//	input row (A) ──► output rows (A, S2), (A, S3)
//	input row (B) ──► output rows (B, S1), (B, S2)
func applyStepFanoutCTEs(
	q *chsql.SelectQuery,
	start, end time.Time,
	step, window, offset time.Duration,
	timeColumn string,
) *chsql.SelectQuery {
	stepMS := step.Milliseconds()
	if stepMS <= 0 {
		stepMS = window.Milliseconds()
		if stepMS <= 0 {
			stepMS = 1
		}
	}
	windowMS := window.Milliseconds()
	offsetMS := offset.Milliseconds()
	toInt64 := chsql.ToInt64Val[int64]

	return q.
		// Query start/end aligned to step boundaries.
		With("first_step_ms", toInt64(start.UnixMilli())).
		With("last_step_ms", toInt64(end.UnixMilli())).
		// Step and range window sizes.
		With("step_ms", toInt64(stepMS)).
		With("window_ms", toInt64(windowMS)).
		// PromQL/LogQL offset: the query window is shifted back by this amount.
		With("offset_ms", toInt64(offsetMS)).
		// Raw row timestamp in milliseconds.
		With("point_ms", chsql.ToUnixTimestamp64Milli(chsql.Ident(timeColumn))).
		// point_ms + offset_ms: because offset shifts the query window back by offset_ms,
		// adding it to the row time is equivalent — a row at T belongs to step S if
		// S - window_ms <= point_offset_ms < S, which avoids carrying offset through every check.
		With("point_offset_ms", chsql.Add(chsql.Ident("point_ms"), chsql.Ident("offset_ms"))).
		// ceil((point_offset_ms - first_step_ms) / step_ms) * step_ms, clamped to 0:
		// distance in ms from first_step_ms to the earliest step this row could belong to.
		With("offset_from_start", chsql.Greatest(
			chsql.ToInt64(chsql.Integer(0)),
			chsql.Sub(
				chsql.Add(chsql.Ident("point_offset_ms"), chsql.Sub(chsql.Ident("step_ms"), chsql.Integer(1))),
				chsql.Ident("first_step_ms"),
			),
		)).
		// floor(window_ms / step_ms) + 1: max steps a single row can fall into.
		With("num_steps", chsql.ToUInt64(chsql.Add(chsql.IntDiv(chsql.Ident("window_ms"), chsql.Ident("step_ms")), chsql.Integer(1)))).
		// first_step_ms + intDiv(offset_from_start, step_ms) * step_ms:
		// timestamp of the earliest step this row belongs to, snapped to the step grid.
		With("first_sample_step_ms", chsql.Add(
			chsql.Ident("first_step_ms"),
			chsql.Mul(chsql.IntDiv(chsql.Ident("offset_from_start"), chsql.Ident("step_ms")), chsql.Ident("step_ms")),
		))
}
