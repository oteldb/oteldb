package prome2e_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/prompb"
	oteldbpromql "github.com/oteldb/oteldb/internal/promql"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/prometheusremotewrite"
)

// BenchmarkPromQLNodeExporter benchmarks three representative node_exporter PromQL queries
// (count_cpu_cores, cpu_usage, full_scan_count — the canonical set from the oteldb/benchmark suite)
// end-to-end against the embedded storage engine.
//
// The dataset is a simulated node_exporter scrape: neHosts synthetic hosts (instance="host-N",
// job="node_exporter"), each exporting the usual node_cpu_seconds_total / memory / disk / network /
// filesystem / load series. It is converted to OTLP through oteldb's real Prometheus remote-write
// translation (prometheusremotewrite.FromTimeSeries), ingested through the OTLP storage sink, then
// each query runs through the PromQL engine over the storage fetch seam.
func BenchmarkPromQLNodeExporter(b *testing.B) {
	ctx := context.Background()

	end := time.Now().Truncate(neStep)
	tss := genNodeExporter(end)

	store, err := storage.InMemory()
	require.NoError(b, err)
	b.Cleanup(func() { _ = store.Close(ctx) })
	backend := storagebackend.New(store)

	// Convert to OTLP via oteldb's real remote-write ingest translation and consume it. Batch the
	// series so each pmetric.Metrics stays a sane size.
	const batch = 500
	for i := 0; i < len(tss); i += batch {
		j := min(i+batch, len(tss))
		md, err := prometheusremotewrite.FromTimeSeries(tss[i:j], prometheusremotewrite.Settings{})
		require.NoError(b, err)
		require.NoError(b, backend.ConsumeMetrics(ctx, md))
	}

	engine, err := oteldbpromql.New(backend, oteldbpromql.EngineOpts{
		MaxSamples:           50_000_000,
		Timeout:              time.Minute,
		LookbackDelta:        5 * time.Minute,
		EnableNegativeOffset: true,
	})
	require.NoError(b, err)

	opts := promql.NewPrometheusQueryOpts(false, 0)
	start := end.Add(-2 * time.Minute) // matches the suite's 120s lookback for the range query
	step := 15 * time.Second

	// Queries are verbatim from queries/metrics.promql.yml in the oteldb/benchmark repo.
	for _, tt := range []struct {
		name    string
		query   string
		instant bool
	}{
		{
			name:    "count_cpu_cores",
			query:   `count(count(node_cpu_seconds_total{job="node_exporter"}) by (cpu))`,
			instant: true,
		},
		{
			name:    "cpu_usage",
			query:   `sum by(instance) (irate(node_cpu_seconds_total{job="node_exporter",mode="user"}[1m])) / on(instance) group_left sum by(instance) (irate(node_cpu_seconds_total{job="node_exporter"}[1m]))`,
			instant: false,
		},
		{
			name:    "full_scan_count",
			query:   `count({__name__=~"node_.+"})`,
			instant: true,
		},
	} {
		b.Run(tt.name, func(b *testing.B) {
			exec := func() *promql.Result {
				var (
					q   promql.Query
					err error
				)
				if tt.instant {
					q, err = engine.NewInstantQuery(ctx, backend, opts, tt.query, end)
				} else {
					q, err = engine.NewRangeQuery(ctx, backend, opts, tt.query, start, end, step)
				}
				require.NoError(b, err)
				defer q.Close()
				res := q.Exec(ctx)
				require.NoError(b, res.Err)
				return res
			}

			// Sanity-check the query returns data before timing it.
			require.Positive(b, resultLen(exec()), "query %q returned no data", tt.query)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = exec()
			}
		})
	}
}

// resultLen returns the number of series/samples in a PromQL result, regardless of value type.
func resultLen(res *promql.Result) int {
	switch v := res.Value.(type) {
	case promql.Matrix:
		return len(v)
	case promql.Vector:
		return len(v)
	case promql.Scalar:
		return 1
	default:
		return 0
	}
}

// node_exporter dataset shape.
const (
	neHosts  = 10
	neCores  = 8
	neWindow = 10 * time.Minute
	neStep   = 15 * time.Second
)

// genNodeExporter builds a simulated node_exporter dataset as Prometheus remote-write TimeSeries.
// Counters increase monotonically across [end-neWindow, end] (so rate/irate are well defined) and
// gauges wander deterministically. Labels match a real node_exporter scrape: job="node_exporter",
// instance="host-N", plus per-metric dimensions (cpu, mode, device, mountpoint, ...).
func genNodeExporter(end time.Time) []prompb.TimeSeries {
	rng := rand.New(rand.NewSource(1))

	var stamps []int64
	for t := end.Add(-neWindow); !t.After(end); t = t.Add(neStep) {
		stamps = append(stamps, t.UnixMilli())
	}
	stepSec := neStep.Seconds()

	var out []prompb.TimeSeries
	add := func(name string, base []prompb.Label, extra []prompb.Label, kind neKind, rate, baseVal, amp float64) {
		labels := append([]prompb.Label{neLabel("__name__", name)}, base...)
		labels = append(labels, extra...)
		sort.Slice(labels, func(i, j int) bool { return string(labels[i].Name) < string(labels[j].Name) })
		out = append(out, prompb.TimeSeries{
			Labels:  labels,
			Samples: neSamples(stamps, stepSec, kind, rate, baseVal, amp, rng),
		})
	}

	cpuModes := []string{"user", "system", "idle", "iowait", "nice", "irq", "softirq", "steal"}
	disks := []string{"sda", "sdb", "nvme0n1"}
	netDevs := []string{"eth0", "eth1", "lo"}
	mounts := []struct{ mountpoint, device, fstype string }{
		{"/", "/dev/sda1", "ext4"},
		{"/home", "/dev/sdb1", "xfs"},
		{"/boot", "/dev/sda2", "ext4"},
	}

	for h := range neHosts {
		hl := []prompb.Label{neLabel("instance", fmt.Sprintf("host-%d", h)), neLabel("job", "node_exporter")}

		for c := range neCores {
			cpu := fmt.Sprintf("%d", c)
			for _, mode := range cpuModes {
				add("node_cpu_seconds_total", hl, []prompb.Label{neLabel("cpu", cpu), neLabel("mode", mode)}, neCounter, neModeRate(mode, rng), rng.Float64()*1000, 0)
			}
			add("node_schedstat_waiting_seconds_total", hl, []prompb.Label{neLabel("cpu", cpu)}, neCounter, 0.05+rng.Float64()*0.1, rng.Float64()*50, 0)
		}
		for _, d := range disks {
			dl := []prompb.Label{neLabel("device", d)}
			add("node_disk_read_bytes_total", hl, dl, neCounter, 5e5+rng.Float64()*2e6, rng.Float64()*1e9, 0)
			add("node_disk_written_bytes_total", hl, dl, neCounter, 4e5+rng.Float64()*1.5e6, rng.Float64()*1e9, 0)
			add("node_disk_io_time_seconds_total", hl, dl, neCounter, 0.1+rng.Float64()*0.3, rng.Float64()*100, 0)
			add("node_disk_reads_completed_total", hl, dl, neCounter, 20+rng.Float64()*100, rng.Float64()*1e6, 0)
			add("node_disk_writes_completed_total", hl, dl, neCounter, 15+rng.Float64()*80, rng.Float64()*1e6, 0)
		}
		for _, n := range netDevs {
			nl := []prompb.Label{neLabel("device", n)}
			add("node_network_receive_bytes_total", hl, nl, neCounter, 1e6+rng.Float64()*5e6, rng.Float64()*1e10, 0)
			add("node_network_transmit_bytes_total", hl, nl, neCounter, 8e5+rng.Float64()*4e6, rng.Float64()*1e10, 0)
			add("node_network_receive_packets_total", hl, nl, neCounter, 1e3+rng.Float64()*5e3, rng.Float64()*1e7, 0)
			add("node_network_transmit_packets_total", hl, nl, neCounter, 9e2+rng.Float64()*4e3, rng.Float64()*1e7, 0)
		}
		for _, m := range mounts {
			ml := []prompb.Label{neLabel("device", m.device), neLabel("fstype", m.fstype), neLabel("mountpoint", m.mountpoint)}
			size := 100e9 + rng.Float64()*900e9
			add("node_filesystem_size_bytes", hl, ml, neGauge, 0, size, 0)
			add("node_filesystem_avail_bytes", hl, ml, neGauge, 0, size*0.4, size*0.05)
			add("node_filesystem_free_bytes", hl, ml, neGauge, 0, size*0.45, size*0.05)
			add("node_filesystem_files", hl, ml, neGauge, 0, 5e6, 0)
			add("node_filesystem_files_free", hl, ml, neGauge, 0, 4e6, 1e5)
		}
		memTotal := 32e9 + float64(h%4)*16e9
		add("node_memory_MemTotal_bytes", hl, nil, neGauge, 0, memTotal, 0)
		add("node_memory_MemFree_bytes", hl, nil, neGauge, 0, memTotal*0.3, memTotal*0.05)
		add("node_memory_MemAvailable_bytes", hl, nil, neGauge, 0, memTotal*0.5, memTotal*0.05)
		add("node_memory_Cached_bytes", hl, nil, neGauge, 0, memTotal*0.15, memTotal*0.03)
		add("node_memory_Buffers_bytes", hl, nil, neGauge, 0, memTotal*0.05, memTotal*0.01)
		add("node_load1", hl, nil, neGauge, 0, 1+rng.Float64()*float64(neCores), 1)
		add("node_load5", hl, nil, neGauge, 0, 1+rng.Float64()*float64(neCores), 0.5)
		add("node_load15", hl, nil, neGauge, 0, 1+rng.Float64()*float64(neCores), 0.25)
		add("node_context_switches_total", hl, nil, neCounter, 5e3+rng.Float64()*2e4, rng.Float64()*1e8, 0)
		add("node_intr_total", hl, nil, neCounter, 4e3+rng.Float64()*1e4, rng.Float64()*1e8, 0)
		add("node_forks_total", hl, nil, neCounter, 5+rng.Float64()*30, rng.Float64()*1e6, 0)
		add("node_procs_running", hl, nil, neGauge, 0, 1+rng.Float64()*4, 2)
		add("node_procs_blocked", hl, nil, neGauge, 0, rng.Float64()*2, 1)
	}
	return out
}

type neKind int

const (
	neCounter neKind = iota
	neGauge
)

func neLabel(name, value string) prompb.Label {
	return prompb.Label{Name: []byte(name), Value: []byte(value)}
}

func neModeRate(mode string, rng *rand.Rand) float64 {
	switch mode {
	case "idle":
		return 0.6 + rng.Float64()*0.3
	case "user":
		return 0.1 + rng.Float64()*0.2
	case "system":
		return 0.05 + rng.Float64()*0.1
	case "iowait":
		return rng.Float64() * 0.05
	default:
		return rng.Float64() * 0.02
	}
}

func neSamples(stamps []int64, stepSec float64, kind neKind, rate, base, amp float64, rng *rand.Rand) []prompb.Sample {
	samples := make([]prompb.Sample, 0, len(stamps))
	phase := rng.Float64() * 2 * math.Pi
	val := base
	for i, ts := range stamps {
		switch kind {
		case neCounter:
			if i > 0 {
				val += rate * stepSec * (0.85 + 0.3*rng.Float64()) // jitter so irate isn't perfectly flat
			}
		case neGauge:
			val = base + amp*math.Sin(phase+float64(i)*0.3)
			if val < 0 {
				val = 0
			}
		}
		samples = append(samples, prompb.Sample{Value: val, Timestamp: ts})
	}
	return samples
}
