package storagebackend_test

import (
	"context"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	oteldbstorage "github.com/oteldb/storage"
	"github.com/oteldb/storage/readbudget"

	"github.com/oteldb/oteldb/internal/scarecrow"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

type partitionBudgetSpy struct {
	scarecrow.Scanner
	partitions int
	limit      int64
}

func (s *partitionBudgetSpy) WithQueryBudget(ctx context.Context) context.Context {
	ctx = s.Scanner.(scarecrow.QueryBudgetScanner).WithQueryBudget(ctx)
	s.limit = readbudget.From(ctx).Remaining()
	return ctx
}

func (s *partitionBudgetSpy) ScanPartitions(ctx context.Context, mint, maxt int64, matchers []*labels.Matcher, workers int) ([]scarecrow.PartitionIterator, error) {
	s.partitions++
	return s.Scanner.(scarecrow.PartitionScanner).ScanPartitions(ctx, mint, maxt, matchers, workers)
}

func TestScarecrowParallelNativeQueryBudget(t *testing.T) {
	previous := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	store, err := oteldbstorage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.WithoutCancel(t.Context()))) })
	base := time.Unix(2_000_000, 0).UTC()
	metrics := pmetric.NewMetrics()
	metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("budget_gauge")
	points := metric.SetEmptyGauge().DataPoints()
	for i := range 8 {
		for j := range 64 {
			point := points.AppendEmpty()
			point.Attributes().PutStr("instance", strconv.Itoa(i))
			point.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(j) * time.Second)))
			point.SetDoubleValue(float64(j))
		}
	}
	require.NoError(t, storagebackend.New(store).ConsumeMetrics(t.Context(), metrics))
	for _, name := range []string{"automatic", "tiny", "near-full"} {
		t.Run(name, func(t *testing.T) {
			if name == "automatic" && previous < 2 {
				t.Skip("parallel head reservation requires at least two process concurrency slots")
			}
			source := &budgetSpy{Source: store}
			backend := storagebackend.NewQuery(source, storagebackend.WithOverTimePushdown(false))
			scanner := &partitionBudgetSpy{Scanner: backend.ScarecrowScanner()}
			engine := scarecrow.NewEngine(scarecrow.Opts{NewScanner: func(storage.Queryable) scarecrow.Scanner { return scanner }})
			ctx := t.Context()
			var supplied *readbudget.Budget
			var held int64
			switch name {
			case "tiny":
				supplied = readbudget.New(1)
			case "near-full":
				supplied = readbudget.New(1 << 20)
				held = (1 << 20) - 1
				require.NoError(t, supplied.Reserve(held))
			}
			if supplied != nil {
				ctx = readbudget.With(ctx, supplied)
			}
			query, err := engine.NewRangeQuery(ctx, backend, nil, `avg(irate(budget_gauge[5s]))`,
				base.Add(time.Second), base.Add(63*time.Second), time.Second)
			require.NoError(t, err)
			result := query.Exec(ctx)
			query.Close()
			require.NoError(t, result.Err)
			require.Equal(t, 1, source.calls, "one install covers schema resolution and every chunk")
			require.Len(t, source.budgets, 1)
			budget := source.budgets[0]
			if supplied != nil {
				require.Same(t, supplied, budget, "the caller's allowance must survive the native hook")
				require.Zero(t, scanner.partitions, "insufficient head space must use one serial scan")
				budget.Release(held)
			} else {
				require.Positive(t, scanner.partitions)
				require.Positive(t, budget.Peak())
			}
			want := scanner.limit + held
			require.Equal(t, want, budget.Remaining(), "query reservations must all be returned")
			require.NoError(t, budget.Reserve(want))
			budget.Release(want)
		})
	}
}
