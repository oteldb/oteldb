package chstorage

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestExtrapolatedRateValue(t *testing.T) {
	tests := []struct {
		name string
		in   rateWindow
		want float64
		ok   bool
	}{
		{
			name: "full_range",
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
			name: "counter_reset",
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
			name: "boundary_extrapolation",
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
			name: "single_sample",
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
			got, ok := extrapolatedRateValue(tt.in)
			if ok != tt.ok {
				t.Fatalf("extrapolatedRateValue() ok = %v, want %v", ok, tt.ok)
			}
			if !ok {
				return
			}
			if got != tt.want {
				t.Fatalf("extrapolatedRateValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryRatePointsSQL(t *testing.T) {
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

	var hash [16]byte
	hash[0] = 1
	_, err := p.queryRatePoints(
		context.Background(),
		time.Unix(60, 0),
		time.Unix(120, 0),
		time.Minute,
		5*time.Minute,
		0,
		map[[16]byte]labels.Labels{
			hash: labels.FromStrings("__name__", "requests_total"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{
		"arrayJoin(arrayFilter",
		"arrayMap(x -> x.2, arraySort",
		"arrayPopFront(vals)",
		"arrayPopBack(vals)",
		"reinterpretAsUInt64(value)",
		"GROUP BY hash,step_ms_val",
		"HAVING (samples > 1)",
		"ORDER BY hash ASC,step_ts ASC",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("generated SQL does not contain %q:\n%s", want, got)
		}
	}
}
