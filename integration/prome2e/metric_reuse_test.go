package prome2e_test

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/otlp/pdataconv"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestStorageMetricsBatchReuse(t *testing.T) {
	for _, consumer := range []bool{false, true} {
		t.Run("consumer="+strconv.FormatBool(consumer), func(t *testing.T) {
			ctx := t.Context()
			dir := t.TempDir()
			be, err := file.New(filepath.Join(dir, "parts"))
			require.NoError(t, err)
			opts := storage.Options{
				Backend: be, WALDir: filepath.Join(dir, "wal"), FlushInterval: -1, FlushThresholdBytes: -1,
			}
			store, err := storage.Open(ctx, opts)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
			backend := storagebackend.New(store)
			at := time.Unix(2_000_000_000, 0)
			var native metric.Metrics
			for round, points := range []int{8, 1, 0} {
				marker := "round" + strconv.Itoa(round)
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				rm.SetSchemaUrl(marker)
				rm.Resource().Attributes().PutStr("resource_marker", marker)
				sm := rm.ScopeMetrics().AppendEmpty()
				sm.Scope().SetName(marker)
				sm.Scope().Attributes().PutStr("scope_marker", marker)
				for point := range points {
					m := sm.Metrics().AppendEmpty()
					m.SetName("reuse_" + marker)
					dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
					dp.SetTimestamp(pcommon.Timestamp(at.UnixNano()))
					dp.SetDoubleValue(float64(point + 42))
					dp.Attributes().PutStr("slot", strconv.Itoa(point))
					ex := dp.Exemplars().AppendEmpty()
					ex.SetTimestamp(dp.Timestamp())
					ex.SetDoubleValue(float64(point + 1))
					ex.FilteredAttributes().PutStr("exemplar_marker", marker)
				}
				if consumer {
					require.NoError(t, backend.ConsumeMetrics(ctx, md))
				} else {
					// Force descriptor reuse independently of sync.Pool scheduling.
					native.Reset()
					require.Zero(t, pdataconv.AppendMetrics(&native, md).Points)
					_, err := store.WriteMetrics(ctx, native)
					require.NoError(t, err)
				}
				rm.Resource().Attributes().Clear()
				sm.Scope().Attributes().Clear()
				sm.Metrics().RemoveIf(func(pmetric.Metric) bool { return true })
			}
			native.Reset()

			check := func() {
				t.Helper()
				_, client := setupDB(t, integration.TraceProvider(t), backend)
				for round, count := range []int{8, 1} {
					marker := "round" + strconv.Itoa(round)
					r, err := client.GetQuery(ctx, promapi.GetQueryParams{
						Query: "reuse_" + marker, Time: promapi.NewOptPrometheusTimestamp(getPromTS(pcommon.Timestamp(at.UnixNano()))),
					})
					require.NoError(t, err)
					require.Equal(t, promapi.VectorData, r.Data.Type)
					require.Len(t, r.Data.Vector.Result, count)
					for _, point := range r.Data.Vector.Result {
						slot, err := strconv.Atoi(point.Metric["slot"])
						require.NoError(t, err)
						value, ok := point.Value.HistogramOrValue.GetStringFloat64()
						require.True(t, ok)
						require.Equal(t, float64(slot+42), float64(value))
						require.Equal(t, marker, point.Metric["resource_marker"])
						require.Equal(t, marker, point.Metric["scope_marker"])
					}
					q, err := backend.ExemplarQuerier(ctx)
					require.NoError(t, err)
					exs, err := q.Select(at.UnixMilli()-1, at.UnixMilli()+1,
						[]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "reuse_"+marker)})
					require.NoError(t, err)
					require.Len(t, exs, count)
					for _, result := range exs {
						require.Len(t, result.Exemplars, 1)
						require.Equal(t, marker, result.Exemplars[0].Labels.Get("exemplar_marker"))
					}
				}
			}
			check()
			require.NoError(t, store.Admin().Flush(ctx, "default", signal.Metric))
			check()
			require.NoError(t, store.Close(ctx))
			store, err = storage.Open(ctx, opts)
			require.NoError(t, err)
			backend = storagebackend.New(store)
			check()
		})
	}
}
