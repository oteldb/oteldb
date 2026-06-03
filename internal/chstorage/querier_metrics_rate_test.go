package chstorage

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestExtrapolatedValue(t *testing.T) {
	tests := []struct {
		name string
		in   rateWindow
		kind rateKind
		want float64
		ok   bool
	}{
		// rate() cases
		{
			name: "rate/full_range",
			kind: rateKindRate,
			in: rateWindow{
				StepTime: 60_000,
				Range:    60_000,
				FirstT:   0,
				FirstV:   0,
				LastT:    60_000,
				LastV:    60,
				Samples:  2,
			},
			want: 1,
			ok:   true,
		},
		{
			name: "rate/counter_reset",
			kind: rateKindRate,
			in: rateWindow{
				StepTime: 60_000,
				Range:    60_000,
				FirstT:   0,
				FirstV:   90,
				LastT:    60_000,
				LastV:    20,
				ResetSum: 100,
				Samples:  3,
			},
			want: 0.5,
			ok:   true,
		},
		{
			name: "rate/boundary_extrapolation",
			kind: rateKindRate,
			in: rateWindow{
				StepTime: 60_000,
				Range:    60_000,
				FirstT:   10_000,
				FirstV:   10,
				LastT:    50_000,
				LastV:    50,
				Samples:  5,
			},
			want: 1,
			ok:   true,
		},
		{
			name: "rate/single_sample",
			kind: rateKindRate,
			in: rateWindow{
				StepTime: 60_000,
				Range:    60_000,
				FirstT:   30_000,
				FirstV:   1,
				LastT:    30_000,
				LastV:    1,
				Samples:  1,
			},
			ok: false,
		},

		// increase() cases — same formula as rate but no division by range seconds
		{
			name: "increase/full_range",
			kind: rateKindIncrease,
			in: rateWindow{
				StepTime: 60_000,
				Range:    60_000,
				FirstT:   0,
				FirstV:   0,
				LastT:    60_000,
				LastV:    60,
				Samples:  2,
			},
			// rate() = 1/s, increase() = 1/s * 60s = 60
			want: 60,
			ok:   true,
		},
		{
			name: "increase/counter_reset",
			kind: rateKindIncrease,
			in: rateWindow{
				StepTime: 60_000,
				Range:    60_000,
				FirstT:   0,
				FirstV:   90,
				LastT:    60_000,
				LastV:    20,
				ResetSum: 100,
				Samples:  3,
			},
			// rate() = 0.5/s, increase() = 0.5 * 60 = 30
			want: 30,
			ok:   true,
		},

		// delta() cases — gauge, no counter-reset, no division by seconds
		{
			name: "delta/full_range",
			kind: rateKindDelta,
			in: rateWindow{
				StepTime: 60_000,
				Range:    60_000,
				FirstT:   0,
				FirstV:   10,
				LastT:    60_000,
				LastV:    40,
				// ResetSum is ignored for delta.
				ResetSum: 999,
				Samples:  2,
			},
			// delta = lastV - firstV = 30, no division by seconds, extrapolation factor = 1.
			want: 30,
			ok:   true,
		},
		{
			name: "delta/gauge_decrease",
			kind: rateKindDelta,
			in: rateWindow{
				StepTime: 60_000,
				Range:    60_000,
				FirstT:   0,
				FirstV:   50,
				LastT:    60_000,
				LastV:    20,
				Samples:  2,
			},
			// Gauges can decrease; delta = 20 - 50 = -30.
			want: -30,
			ok:   true,
		},
		{
			name: "delta/single_sample",
			kind: rateKindDelta,
			in: rateWindow{
				StepTime: 60_000,
				Range:    60_000,
				FirstT:   30_000,
				FirstV:   1,
				LastT:    30_000,
				LastV:    1,
				Samples:  1,
			},
			ok: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := extrapolatedValue(tt.in, tt.kind)
			if ok != tt.ok {
				t.Fatalf("extrapolatedValue() ok = %v, want %v", ok, tt.ok)
			}
			if !ok {
				return
			}
			if got != tt.want {
				t.Fatalf("extrapolatedValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInstantValue(t *testing.T) {
	tests := []struct {
		name string
		in   instantWindow
		kind rateKind
		want float64
		ok   bool
	}{
		// irate() cases
		{
			name: "irate/normal",
			kind: rateKindIRate,
			in: instantWindow{
				LastT:     20_000,
				LastV:     20,
				PrevLastT: 10_000,
				PrevLastV: 10,
				Samples:   2,
			},
			// dt = 10s, delta = 10 → rate = 1/s
			want: 1,
			ok:   true,
		},
		{
			name: "irate/counter_reset",
			kind: rateKindIRate,
			in: instantWindow{
				LastT:     20_000,
				LastV:     5,
				PrevLastT: 10_000,
				PrevLastV: 100,
				Samples:   2,
			},
			// Reset detected: delta = lastV = 5, dt = 10s → rate = 0.5/s
			want: 0.5,
			ok:   true,
		},
		{
			name: "irate/single_sample",
			kind: rateKindIRate,
			in: instantWindow{
				LastT:     10_000,
				LastV:     5,
				PrevLastT: 10_000,
				PrevLastV: 5,
				Samples:   1,
			},
			ok: false,
		},
		{
			name: "irate/same_timestamp",
			kind: rateKindIRate,
			in: instantWindow{
				LastT:     10_000,
				LastV:     5,
				PrevLastT: 10_000,
				PrevLastV: 3,
				Samples:   2,
			},
			ok: false,
		},

		// idelta() cases
		{
			name: "idelta/increase",
			kind: rateKindIDelta,
			in: instantWindow{
				LastT:     20_000,
				LastV:     30,
				PrevLastT: 10_000,
				PrevLastV: 10,
				Samples:   2,
			},
			want: 20,
			ok:   true,
		},
		{
			name: "idelta/decrease",
			kind: rateKindIDelta,
			in: instantWindow{
				LastT:     20_000,
				LastV:     10,
				PrevLastT: 10_000,
				PrevLastV: 30,
				Samples:   2,
			},
			// Gauges can decrease; no counter-reset adjustment for idelta.
			want: -20,
			ok:   true,
		},
		{
			name: "idelta/single_sample",
			kind: rateKindIDelta,
			in: instantWindow{
				LastT:     10_000,
				LastV:     5,
				PrevLastT: 10_000,
				PrevLastV: 5,
				Samples:   1,
			},
			ok: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := instantValue(tt.in, tt.kind)
			if ok != tt.ok {
				t.Fatalf("instantValue() ok = %v, want %v", ok, tt.ok)
			}
			if !ok {
				return
			}
			if got != tt.want {
				t.Fatalf("instantValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryRatePointsSQL(t *testing.T) {
	var hash [16]byte
	hash[0] = 1
	ts := map[[16]byte]labels.Labels{
		hash: labels.FromStrings("__name__", "requests_total"),
	}

	runQuery := func(t *testing.T, kind rateKind) string {
		t.Helper()
		var got string
		p := &promQuerier{
			tables: DefaultTables(),
			tracer: noop.NewTracerProvider().Tracer("test"),
			do: func(ctx context.Context, s selectQuery) error {
				query, err := s.Query.Prepare(s.OnResult)
				if err != nil {
					return err
				}
				got = query.Body
				return nil
			},
		}
		_, err := p.queryRatePoints(
			context.Background(),
			time.Unix(60, 0),
			time.Unix(120, 0),
			time.Minute,
			5*time.Minute,
			0,
			ts,
			kind,
		)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}

	t.Run("rate", func(t *testing.T) {
		got := runQuery(t, rateKindRate)
		for _, want := range []string{
			"arrayJoin(arrayFilter",
			"lagInFrame(value, 1, value) OVER (PARTITION BY hash,step_ms_val ORDER BY timestamp ASC)",
			"sumIf(prev_value, (value < prev_value))",
			"reinterpretAsUInt64(value)",
			"GROUP BY hash,step_ms_val",
			"HAVING (samples > 1)",
			"ORDER BY hash ASC,step_ts ASC",
		} {
			if !strings.Contains(got, want) {
				t.Fatalf("rate SQL does not contain %q:\n%s", want, got)
			}
		}
	})

	t.Run("increase", func(t *testing.T) {
		got := runQuery(t, rateKindIncrease)
		// Same SQL structure as rate (counter-reset detection enabled).
		for _, want := range []string{
			"arrayJoin(arrayFilter",
			"lagInFrame(value, 1, value) OVER (PARTITION BY hash,step_ms_val ORDER BY timestamp ASC)",
			"sumIf(prev_value, (value < prev_value))",
			"GROUP BY hash,step_ms_val",
			"HAVING (samples > 1)",
		} {
			if !strings.Contains(got, want) {
				t.Fatalf("increase SQL does not contain %q:\n%s", want, got)
			}
		}
	})

	t.Run("delta", func(t *testing.T) {
		got := runQuery(t, rateKindDelta)
		// delta skips the window function — no lagInFrame or sumIf.
		for _, absent := range []string{
			"lagInFrame",
			"sumIf",
		} {
			if strings.Contains(got, absent) {
				t.Fatalf("delta SQL should not contain %q:\n%s", absent, got)
			}
		}
		// Still needs fan-out and aggregation.
		for _, want := range []string{
			"arrayJoin(arrayFilter",
			"GROUP BY hash,step_ms_val",
			"HAVING (samples > 1)",
		} {
			if !strings.Contains(got, want) {
				t.Fatalf("delta SQL does not contain %q:\n%s", want, got)
			}
		}
	})

	t.Run("irate", func(t *testing.T) {
		got := runQuery(t, rateKindIRate)
		// irate uses the extended window function with prev_timestamp.
		for _, want := range []string{
			"arrayJoin(arrayFilter",
			"lagInFrame(value, 1, value) OVER (PARTITION BY hash,step_ms_val ORDER BY timestamp ASC)",
			"lagInFrame(timestamp, 1, timestamp) OVER (PARTITION BY hash,step_ms_val ORDER BY timestamp ASC)",
			"argMax(prev_value, timestamp)",
			"argMax(prev_timestamp, timestamp)",
			"GROUP BY hash,step_ms_val",
			"HAVING (samples > 1)",
		} {
			if !strings.Contains(got, want) {
				t.Fatalf("irate SQL does not contain %q:\n%s", want, got)
			}
		}
		// irate does not produce reset_sum or first_pair.
		for _, absent := range []string{
			"sumIf",
			"first_pair",
		} {
			if strings.Contains(got, absent) {
				t.Fatalf("irate SQL should not contain %q:\n%s", absent, got)
			}
		}
	})

	t.Run("idelta", func(t *testing.T) {
		got := runQuery(t, rateKindIDelta)
		// idelta uses the same extended window function as irate.
		for _, want := range []string{
			"arrayJoin(arrayFilter",
			"lagInFrame(value, 1, value) OVER (PARTITION BY hash,step_ms_val ORDER BY timestamp ASC)",
			"lagInFrame(timestamp, 1, timestamp) OVER (PARTITION BY hash,step_ms_val ORDER BY timestamp ASC)",
			"argMax(prev_value, timestamp)",
			"argMax(prev_timestamp, timestamp)",
			"GROUP BY hash,step_ms_val",
			"HAVING (samples > 1)",
		} {
			if !strings.Contains(got, want) {
				t.Fatalf("idelta SQL does not contain %q:\n%s", want, got)
			}
		}
	})
}
