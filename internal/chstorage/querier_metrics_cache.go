package chstorage

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/oteldb/internal/metricscache"
)

type fetchPointsFunc func(ctx context.Context, fetchStart, fetchEnd time.Time) (map[[16]byte]*series[pointData], error)

func (p *promQuerier) fetchAndMergeCache(
	ctx context.Context,
	span trace.Span,
	start, end time.Time,
	step time.Duration,
	fn string,
	timeseries map[[16]byte]labels.Labels,
	fetch fetchPointsFunc,
) (map[[16]byte]*series[pointData], error) {
	cacheEnd := time.Now().Add(-p.metricsCache.SafetyLag())
	if end.Before(cacheEnd) {
		cacheEnd = end
	}

	// 1. Determine per-series watermarks and global fetch lower bound.
	seriesWatermark, globalFetchFrom, stats := computeFetchRange(
		ctx,
		p.metricsCache,
		step,
		fn,
		start.UnixMilli(),
		timeseries,
	)

	span.SetAttributes(
		attribute.String("chstorage.metrics_cache.function", fn),
		attribute.Int64("chstorage.metrics_cache.cache_end", cacheEnd.UnixMilli()),
		attribute.Int64("chstorage.metrics_cache.global_fetch_from", globalFetchFrom),
		attribute.Int("chstorage.metrics_cache.hits", stats.hits),
		attribute.Int("chstorage.metrics_cache.partial_hits", stats.partialHits),
		attribute.Int("chstorage.metrics_cache.misses", stats.misses),
	)

	// 2. Fetch the "Gap" from ClickHouse.
	fetchStart := time.UnixMilli(globalFetchFrom)
	var fetchedByHash map[[16]byte]*series[pointData]
	if !fetchStart.After(end) {
		var err error
		span.AddEvent("chstorage.fetch_more_series")
		fetchedByHash, err = fetch(ctx, fetchStart, end)
		if err != nil {
			return nil, err
		}

		totalPoints := 0
		for _, s := range fetchedByHash {
			totalPoints += len(s.ts)
		}
		span.AddEvent("chstorage.fetched_more_series", trace.WithAttributes(
			attribute.Int("chstorage.fetched_series", len(fetchedByHash)),
			attribute.Int("chstorage.fetched_points", totalPoints),
		))

		// 3. Update cache for all timeseries.
		bigQuery := p.metricsCache.IsBigQuery(totalPoints)
		span.SetAttributes(attribute.Bool("chstorage.metrics_cache.big_query", bigQuery))
		if bigQuery {
			p.metricsCache.Stats.BigQueries.Add(ctx, 1, cacheTypeOpt(fn, step))
		}
		for hash := range timeseries {
			var (
				ts   []int64
				vals []float64
			)
			if s, ok := fetchedByHash[hash]; ok {
				ts = s.ts
				vals = s.data.values
			}
			p.upsertCache(ctx, hash, step, fn, globalFetchFrom, ts, vals, cacheEnd.UnixMilli(), bigQuery)
		}
	} else {
		span.AddEvent("chstorage.fully_covered_by_cache")
		p.metricsCache.Stats.FullyCovered.Add(ctx, 1, cacheTypeOpt(fn, step))
	}

	// 4. Merge per series: cached[start..watermark] ++ fetched(watermark, end]
	resultMap := make(map[[16]byte]*series[pointData], len(timeseries))
	for hash, lb := range timeseries {
		s := &series[pointData]{labels: lb}
		watermark := seriesWatermark[hash]

		if watermark >= start.UnixMilli() {
			key := metricscache.Key{Hash: hash, Step: step.Milliseconds(), Fn: fn}
			if entry, ok := p.metricsCache.Get(key); ok {
				cachedTS, cachedVals := entry.Slice(start.UnixMilli(), min(watermark, end.UnixMilli()))
				s.ts = append(s.ts, cachedTS...)
				s.data.values = append(s.data.values, cachedVals...)
			}
		}

		if f, ok := fetchedByHash[hash]; ok {
			for i, t := range f.ts {
				if t > watermark {
					s.ts = append(s.ts, t)
					s.data.values = append(s.data.values, f.data.values[i])
				}
			}
		}

		if len(s.ts) > 0 {
			resultMap[hash] = s
		}
	}

	return resultMap, nil
}
