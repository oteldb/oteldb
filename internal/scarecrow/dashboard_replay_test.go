package scarecrow_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

type replayTarget struct {
	Query      string `json:"query"`
	Instant    bool   `json:"instant"`
	End        int64  `json:"end"`
	Start      int64  `json:"start"`
	Step       int64  `json:"step"`
	QueryIndex int    `json:"query_index"`
}

type replaySeries struct {
	Metric map[string]string   `json:"metric"`
	Values [][]json.RawMessage `json:"values"`
	Value  []json.RawMessage   `json:"value"`
}

type replayResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string         `json:"resultType"`
		Result     []replaySeries `json:"result"`
	} `json:"data"`
}

type replaySelection struct {
	labels  []labels.Labels
	samples []scarecrow.Samples
}

// replayScanner holds immutable captured inputs. Its selection cache is primed before timing.
type replayScanner struct {
	series    []scarecrow.Samples
	reference []storage.Series
	mu        sync.RWMutex
	cache     map[string]*replaySelection
}

func loadReplay(t testing.TB, dir string) (*replayScanner, []replayTarget) {
	t.Helper()
	var source struct{ Host string }
	readReplayJSON(t, filepath.Join(dir, "source.json"), &source)
	var response replayResponse
	readReplayJSON(t, filepath.Join(dir, source.Host+".json"), &response)
	require.Equal(t, "success", response.Status)
	require.Equal(t, "matrix", response.Data.ResultType)
	scanner := &replayScanner{cache: make(map[string]*replaySelection)}
	for _, series := range response.Data.Result {
		samples := scarecrow.Samples{Labels: labels.FromMap(series.Metric)}
		for _, pair := range series.Values {
			ts, value := replayPair(t, pair)
			samples.T = append(samples.T, ts)
			samples.V = append(samples.V, value)
		}
		require.True(t, slices.IsSorted(samples.T))
		scanner.series = append(scanner.series, samples)
		chunk := chunkenc.NewXORChunk()
		appender, err := chunk.Appender()
		require.NoError(t, err)
		for i, ts := range samples.T {
			appender.Append(0, ts, samples.V[i])
		}
		scanner.reference = append(scanner.reference,
			&storage.SeriesEntry{Lset: samples.Labels, SampleIteratorFn: chunk.Iterator})
	}
	var targets []replayTarget
	readReplayJSON(t, filepath.Join(dir, "targets.json"), &targets)
	require.Len(t, targets, 242)
	return scanner, targets
}

func readReplayJSON(t testing.TB, name string, dst any) {
	t.Helper()
	data, err := os.ReadFile(name)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, dst))
}

func replayPair(t testing.TB, pair []json.RawMessage) (timestamp int64, value float64) {
	t.Helper()
	require.Len(t, pair, 2)
	var seconds float64
	var text string
	require.NoError(t, json.Unmarshal(pair[0], &seconds))
	require.NoError(t, json.Unmarshal(pair[1], &text))
	value, err := strconv.ParseFloat(text, 64)
	require.NoError(t, err)
	return int64(math.Round(seconds * 1000)), value
}

func (s *replayScanner) selectSeries(mint, maxt int64, matchers []*labels.Matcher) *replaySelection {
	var scratch [1024]byte
	key := binary.BigEndian.AppendUint64(scratch[:0], uint64(mint))
	key = binary.BigEndian.AppendUint64(key, uint64(maxt))
	for _, matcher := range matchers {
		key = append(key, byte(matcher.Type))
		key = binary.AppendUvarint(key, uint64(len(matcher.Name)))
		key = append(key, matcher.Name...)
		key = binary.AppendUvarint(key, uint64(len(matcher.Value)))
		key = append(key, matcher.Value...)
	}
	s.mu.RLock()
	selection, ok := s.cache[string(key)]
	s.mu.RUnlock()
	if ok {
		return selection
	}
	selection = &replaySelection{}
	for _, samples := range s.series {
		matches := true
		for _, matcher := range matchers {
			if !matcher.Matches(samples.Labels.Get(matcher.Name)) {
				matches = false
				break
			}
		}
		if !matches {
			continue
		}
		lo := sort.Search(len(samples.T), func(i int) bool { return samples.T[i] >= mint })
		hi := sort.Search(len(samples.T), func(i int) bool { return samples.T[i] > maxt })
		if lo == hi {
			continue
		}
		samples.T, samples.V = samples.T[lo:hi], samples.V[lo:hi]
		selection.labels = append(selection.labels, samples.Labels)
		selection.samples = append(selection.samples, samples)
	}
	s.mu.Lock()
	s.cache[string(key)] = selection
	s.mu.Unlock()
	return selection
}

func (s *replayScanner) Series(ctx context.Context, mint, maxt int64, matchers []*labels.Matcher) ([]labels.Labels, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return slices.Clone(s.selectSeries(mint, maxt, matchers).labels), nil
}

func (s *replayScanner) Scan(ctx context.Context, mint, maxt int64, matchers []*labels.Matcher) (scarecrow.SeriesIterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &replayIterator{samples: s.selectSeries(mint, maxt, matchers).samples}, nil
}

func (*replayScanner) Close() error { return nil }

type replayIterator struct {
	samples []scarecrow.Samples
	next    int
}

func (it *replayIterator) Next(ctx context.Context) (*scarecrow.Samples, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if it.next == len(it.samples) {
		return nil, nil
	}
	samples := &it.samples[it.next]
	it.next++
	return samples, nil
}

func (*replayIterator) Close() error { return nil }

func (s *replayScanner) queryable() storage.Queryable {
	return &storage.MockQueryable{MockQuerier: &storage.MockQuerier{
		SelectMockFunction: func(sorted bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
			var out []storage.Series
			for _, series := range s.reference {
				matches := true
				for _, matcher := range matchers {
					if !matcher.Matches(series.Labels().Get(matcher.Name)) {
						matches = false
						break
					}
				}
				if matches {
					out = append(out, series)
				}
			}
			if sorted {
				slices.SortFunc(out, func(a, b storage.Series) int { return labels.Compare(a.Labels(), b.Labels()) })
			}
			return &replaySeriesSet{series: out}
		},
	}}
}

type replaySeriesSet struct {
	series []storage.Series
	next   int
}

func (s *replaySeriesSet) Next() bool {
	if s.next == len(s.series) {
		return false
	}
	s.next++
	return true
}

func (s *replaySeriesSet) At() storage.Series              { return s.series[s.next-1] }
func (*replaySeriesSet) Err() error                        { return nil }
func (*replaySeriesSet) Warnings() annotations.Annotations { return nil }
