package chstorage

import (
	"time"

	"github.com/oteldb/oteldb/internal/chstorage/chsql"
)

// nanosPerMilli converts a nanosecond-precision step value (computed by
// [stepFanoutExpr]) back to the millisecond epoch [applyStepFanoutCTEs]'
// callers expect.
const nanosPerMilli = 1_000_000

// stepFanoutExpr is the per-row "step_ms_val" expression shared by every
// step-bucketing offload query (PromQL rate/increase/delta offload, LogQL
// sample aggregation offload). It enumerates every output step (epoch
// milliseconds) whose window (step-window, step] covers the row, given the
// CTEs defined by [applyStepFanoutCTEs]: first_step_ns, last_step_ns,
// step_ns, window_ns, point_offset_ns, first_sample_step_ns, num_steps.
//
// Boundary checks compare nanosecond-precision timestamps, not the
// millisecond-rounded grid values: rows can legitimately sit within a
// fraction of a millisecond of a window edge (e.g. synthetic log generators
// that align timestamps to whole seconds plus a sub-millisecond jitter), and
// rounding the row's own timestamp down to milliseconds before comparing it
// against the window boundary can flip which side of the boundary it falls
// on, silently dropping or duplicating samples at exact step multiples.
//
// A row whose window spans multiple steps (i.e. step < window) produces
// multiple output rows once arrayJoin-ed — see the diagram in
// applyStepFanoutCTEs.
var stepFanoutExpr = func() chsql.Expr {
	var (
		firstStepNSIdent     = chsql.Ident("first_step_ns")
		lastStepNSIdent      = chsql.Ident("last_step_ns")
		stepNSIdent          = chsql.Ident("step_ns")
		windowNSIdent        = chsql.Ident("window_ns")
		pointOffsetNSIdent   = chsql.Ident("point_offset_ns")
		firstSampleStepIdent = chsql.Ident("first_sample_step_ns")
		numStepsIdent        = chsql.Ident("num_steps")
	)

	mapLambda := chsql.Lambda([]string{"i"},
		chsql.Add(firstSampleStepIdent, chsql.Mul(chsql.ColumnToInt64("i"), stepNSIdent)),
	)
	rangeExpr := chsql.Range(numStepsIdent)

	filterLambda := chsql.Lambda([]string{"s"}, chsql.JoinAnd(
		chsql.Gte(chsql.Ident("s"), firstStepNSIdent),
		chsql.Lte(chsql.Ident("s"), lastStepNSIdent),
		chsql.Lt(chsql.Ident("s"), chsql.Add(pointOffsetNSIdent, windowNSIdent)),
	))

	return chsql.IntDiv(
		chsql.ArrayJoin(
			chsql.ArrayFilter(
				filterLambda,
				chsql.ArrayMap(mapLambda, rangeExpr),
			),
		),
		chsql.Integer(nanosPerMilli),
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
	stepNS := step.Nanoseconds()
	if stepNS <= 0 {
		stepNS = window.Nanoseconds()
		if stepNS <= 0 {
			stepNS = int64(time.Millisecond)
		}
	}
	windowNS := window.Nanoseconds()
	offsetNS := offset.Nanoseconds()
	toInt64 := chsql.ToInt64Val[int64]

	return q.
		// Query start/end aligned to step boundaries, in nanoseconds: row
		// timestamps are compared against these at full timestamp precision
		// (see stepFanoutExpr) rather than after rounding to milliseconds,
		// since rounding can flip which side of a window boundary a row
		// falls on.
		With("first_step_ns", toInt64(start.UnixNano())).
		With("last_step_ns", toInt64(end.UnixNano())).
		// Step and range window sizes.
		With("step_ns", toInt64(stepNS)).
		With("window_ns", toInt64(windowNS)).
		// PromQL/LogQL offset: the query window is shifted back by this amount.
		With("offset_ns", toInt64(offsetNS)).
		// Raw row timestamp in nanoseconds.
		With("point_ns", chsql.ToUnixTimestamp64Nano(chsql.Ident(timeColumn))).
		// point_ns + offset_ns: because offset shifts the query window back by offset_ns,
		// adding it to the row time is equivalent — a row at T belongs to step S if
		// S - window_ns <= point_offset_ns < S, which avoids carrying offset through every check.
		With("point_offset_ns", chsql.Add(chsql.Ident("point_ns"), chsql.Ident("offset_ns"))).
		// ceil((point_offset_ns - first_step_ns) / step_ns) * step_ns, clamped to 0:
		// distance in ns from first_step_ns to the earliest step this row could belong to.
		With("offset_from_start", chsql.Greatest(
			chsql.ToInt64(chsql.Integer(0)),
			chsql.Sub(
				chsql.Add(chsql.Ident("point_offset_ns"), chsql.Sub(chsql.Ident("step_ns"), chsql.Integer(1))),
				chsql.Ident("first_step_ns"),
			),
		)).
		// floor(window_ns / step_ns) + 1: max steps a single row can fall into.
		With("num_steps", chsql.ToUInt64(chsql.Add(chsql.IntDiv(chsql.Ident("window_ns"), chsql.Ident("step_ns")), chsql.Integer(1)))).
		// first_step_ns + intDiv(offset_from_start, step_ns) * step_ns:
		// timestamp of the earliest step this row belongs to, snapped to the step grid.
		With("first_sample_step_ns", chsql.Add(
			chsql.Ident("first_step_ns"),
			chsql.Mul(chsql.IntDiv(chsql.Ident("offset_from_start"), chsql.Ident("step_ns")), chsql.Ident("step_ns")),
		))
}
