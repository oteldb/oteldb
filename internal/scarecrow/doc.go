// Package scarecrow is a native, columnar PromQL execution engine over the oteldb/storage
// fetch seam.
//
// Execution is series-major: the currency between operators is a [Column] holding one series'
// values across every step of the current chunk, and [Operator.Next] is called once per output
// series. See docs/promql-engine.md for the design and for why this differs from the
// step-major layout used by the Thanos engine.
//
// The package consumes the upstream github.com/prometheus/prometheus/promql/parser and
// implements [github.com/prometheus/prometheus/promql.QueryEngine], so it is a drop-in for the
// existing engine seam and can be driven by promqltest.
package scarecrow
