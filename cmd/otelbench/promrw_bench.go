package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"github.com/spf13/cobra"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/promapi"
)

type Bench struct {
	addr                       string
	node                       atomic.Pointer[[]byte]
	cfg                        atomic.Pointer[config]
	nodeExporterAddr           string
	clickhouseAddr             string
	queryAddr                  string
	queryInterval              time.Duration
	agentAddr                  string
	targetsCount               int
	pollExporterInterval       time.Duration
	scrapeInterval             time.Duration
	scrapeConfigUpdateInterval time.Duration
	scrapeConfigUpdatePercent  float64
	useVictoria                bool
	targets                    []string
	metricsInfo                atomic.Pointer[metricsInfo]
	storageInfo                atomic.Pointer[ClickhouseStats]
}

type metricsInfo struct {
	Count int
	Size  int
	Hash  string
}

func (s *Bench) PollNodeExporter(ctx context.Context) {
	ticker := time.NewTicker(s.pollExporterInterval)
	defer ticker.Stop()
	if err := s.fetchNodeExporter(ctx); err != nil {
		zctx.From(ctx).Error("cannot fetch node exporter", zap.Error(err))
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.fetchNodeExporter(ctx); err != nil {
				zctx.From(ctx).Error("cannot fetch node exporter", zap.Error(err))
			}
		}
	}
}

func (s *Bench) fetchNodeExporter(ctx context.Context) error {
	u := &url.URL{
		Scheme: "http",
		Path:   "/metrics",
		Host:   s.nodeExporterAddr,
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), http.NoBody)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = res.Body.Close()
	}()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	s.node.Store(&data)

	// Count metrics.
	var metricsCount int
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		metricsCount++
	}
	d := sha256.Sum256(data)
	h := fmt.Sprintf("%x", d[:8])
	s.metricsInfo.Store(&metricsInfo{
		Count: metricsCount,
		Size:  len(data),
		Hash:  h,
	})
	return nil
}

func (s *Bench) HandleConfig(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	v := s.cfg.Load()
	_, _ = w.Write(v.marshalYAML())
}

func (s *Bench) ProgressConfig(ctx context.Context) error {
	// https://github.com/VictoriaMetrics/prometheus-benchmark/blob/50c5891/services/vmagent-config-updater/main.go#L33-L48
	rev := 0
	r := rand.New(rand.NewSource(1)) // #nosec G404
	p := s.scrapeConfigUpdatePercent / 100
	ticker := time.NewTicker(s.scrapeConfigUpdateInterval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			rev++
			revStr := fmt.Sprintf("r%d", rev)
			cfg := s.cfg.Load()
			for _, sc := range cfg.ScrapeConfigs {
				for _, stc := range sc.StaticConfigs {
					if r.Float64() >= p {
						continue
					}
					stc.Labels["revision"] = revStr
				}
			}
			s.cfg.Store(cfg)
		}
	}
}

func (s *Bench) HandleNodeExporter(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	v := s.node.Load()
	if v == nil {
		_, _ = w.Write([]byte("# no data"))
		return
	}
	_, _ = w.Write(*v)
}

func (s *Bench) RunClickhouseReporter(ctx context.Context) error {
	ticker := time.NewTicker(time.Millisecond * 500)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			info, err := fetchClickhouseStats(ctx, s.clickhouseAddr, "metrics_points")
			if err != nil {
				zctx.From(ctx).Error("cannot fetch clickhouse stats", zap.Error(err))
			}
			s.storageInfo.Store(&info)
		}
	}
}

func (s *Bench) RunReporter(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 2)
	defer ticker.Stop()
	var lastHash string
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-ticker.C:
			info := s.metricsInfo.Load()
			if info == nil {
				zctx.From(ctx).Info("no metrics info")
				continue
			}
			{
				if lastHash == info.Hash {
					zctx.From(ctx).Warn("Last hash is the same, node exporter stalled or not working?", zap.String("hash", info.Hash))
				}
				lastHash = info.Hash
			}
			var b strings.Builder
			b.WriteString(fmt.Sprintf("m=%s", fmtInt(info.Count*s.targetsCount)))
			if v := s.storageInfo.Load(); v != nil && s.clickhouseAddr != "" {
				v.WriteInfo(&b, now)
			}
			fmt.Println(b.String())
		}
	}
}

func (s *Bench) RunNodeExporter(ctx context.Context) error {
	args := []string{
		"--no-collector.wifi",
		"--no-collector.hwmon",
		"--no-collector.time",
		"--no-collector.timex",
		"--no-collector.arp",
		"--no-collector.netdev",
		"--no-collector.netstat",
		"--collector.processes",
		"--web.max-requests=40",
		"--web.listen-address=" + s.nodeExporterAddr,
		"--log.format=json",
	}
	// #nosec G204
	cmd := exec.CommandContext(ctx, "node_exporter", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (s *Bench) RunAgent(ctx context.Context) error {
	if len(s.targets) != 1 && s.agentAddr != "" {
		return errors.New("cannot use agentAddr with multiple targets")
	}
	g, ctx := errgroup.WithContext(ctx)
	for i := range s.targets {
		dir, err := os.MkdirTemp("", "vmagent")
		if err != nil {
			return errors.Wrap(err, "create temp dir")
		}
		arg := []string{
			"--httpListenAddr=" + allocateAddr(s.agentAddr),
			"--loggerFormat=json",
			"--remoteWrite.showURL",
			"--promscrape.config=http://" + s.addr + "/config",
			"--remoteWrite.url=" + s.targets[i],
			"--remoteWrite.forceVMProto",
		}
		// #nosec G204
		cmd := exec.CommandContext(ctx, "vmagent", arg...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Dir = dir
		g.Go(func() error {
			defer func() {
				_ = os.RemoveAll(dir)
			}()
			return cmd.Run()
		})
	}
	return g.Wait()
}

func allocateAddr(defaultAddr string) string {
	if defaultAddr != "" {
		return defaultAddr
	}
	var lastErr error
	for i := 0; i < 10; i++ {
		// Listen on random port.
		laddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:")
		if err != nil {
			lastErr = err
			continue
		}
		l, err := net.ListenTCP("tcp", laddr)
		if err != nil {
			lastErr = err
			continue
		}
		addr := l.Addr().String()
		_ = l.Close()
		return addr
	}
	panic("failed to allocate random addr: " + lastErr.Error())
}

func (s *Bench) RunPrometheus(ctx context.Context, dir string) error {
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	prometheusConfigFile := filepath.Join(dir, "prometheus.yml")
	if err := os.WriteFile(prometheusConfigFile, s.cfg.Load().marshalYAML(), 0o600); err != nil {
		return err
	}
	// #nosec G204
	cmd := exec.CommandContext(ctx, "prometheus",
		"--config.file="+filepath.Join(dir, "prometheus.yml"),
		"--web.listen-address="+allocateAddr(s.agentAddr),
		"--enable-feature=agent",
		"--enable-feature=new-service-discovery-manager",
		"--log.format=json",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Dir = dir
	go func() {
		// Periodically update the config.
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := os.WriteFile(prometheusConfigFile, s.cfg.Load().marshalYAML(), 0o600); err != nil {
					zctx.From(ctx).Error("cannot update prometheus config", zap.Error(err))
				}
				if err := cmd.Process.Signal(syscall.SIGHUP); err != nil {
					zctx.From(ctx).Error("cannot send SIGHUP to prometheus", zap.Error(err))
				}
			}
		}
	}()
	return cmd.Run()
}

func (s *Bench) prometheusConfig() *config {
	cfg := newConfig(s.targetsCount, s.scrapeInterval, s.addr)
	if !s.useVictoria {
		var remotes []*remoteWriteConfig
		for i, target := range s.targets {
			remotes = append(remotes, &remoteWriteConfig{
				URL:  target,
				Name: fmt.Sprintf("target-%d", i),
				Metadata: &remoteWriteMetadataConfig{
					Send:         true,
					SendInterval: time.Second,
				},
			})
		}
		cfg.RemoteWrites = remotes
	}
	return cfg
}

func (s *Bench) parseTargets(args []string) error {
	for _, arg := range args {
		u, err := url.Parse(arg)
		if err != nil {
			return errors.Wrap(err, "parse url")
		}
		s.targets = append(s.targets, u.String())
	}
	if len(s.targets) == 0 {
		return errors.New("no targets")
	}
	return nil
}

func (s *Bench) waitForTarget(ctx context.Context, target string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()

	e := backoff.NewExponentialBackOff()
	if err := backoff.RetryNotify(func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, http.NoBody)
		if err != nil {
			return err
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer func() {
			_ = res.Body.Close()
		}()
		zctx.From(ctx).Info("Target ready", zap.String("target", target))
		return nil
	}, backoff.WithContext(e, ctx), func(err error, duration time.Duration) {
		zctx.From(ctx).Warn("cannot fetch target", zap.Error(err), zap.Duration("duration", duration))
	}); err != nil {
		return errors.Wrapf(err, "wait for target %q", target)
	}
	return nil
}

func (s *Bench) run(ctx context.Context) error {
	s.nodeExporterAddr = allocateAddr(s.nodeExporterAddr)
	s.addr = allocateAddr(s.addr)
	s.cfg.Store(s.prometheusConfig())

	// First, wait for target to be available.
	for _, target := range s.targets {
		if err := s.waitForTarget(ctx, target); err != nil {
			return err
		}
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.ProgressConfig(ctx)
	})
	if s.clickhouseAddr != "" {
		g.Go(func() error {
			return s.RunClickhouseReporter(ctx)
		})
	}
	if s.queryAddr != "" {
		g.Go(func() error {
			return s.RunQueryReporter(ctx)
		})
	}
	g.Go(func() error {
		return s.RunReporter(ctx)
	})
	if s.useVictoria {
		g.Go(func() error {
			return s.RunAgent(ctx)
		})
	} else {
		prometheusDir, err := os.MkdirTemp("", "prometheus")
		if err != nil {
			return err
		}
		g.Go(func() error {
			return s.RunPrometheus(ctx, prometheusDir)
		})
	}
	g.Go(func() error {
		return s.RunNodeExporter(ctx)
	})
	g.Go(func() error {
		s.PollNodeExporter(ctx)
		return nil
	})
	g.Go(func() error {
		mux := http.NewServeMux()
		mux.HandleFunc("/node", s.HandleNodeExporter)
		mux.HandleFunc("/config", s.HandleConfig)
		srv := &http.Server{
			Addr:              s.addr,
			Handler:           mux,
			ReadHeaderTimeout: time.Second * 5,
		}
		go func() {
			<-ctx.Done()
			_ = srv.Close()
		}()
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	return g.Wait()
}

func prometheusTimestamp(t time.Time) promapi.PrometheusTimestamp {
	return promapi.PrometheusTimestamp(t.Format(time.RFC3339Nano))
}

type cpuQueryStats struct {
	Count    int
	Duration time.Duration
	Latest   time.Time
}

func (s *Bench) runQueryCPU(ctx context.Context, client promapi.Invoker) (*cpuQueryStats, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*15)
	defer cancel()

	const q = `count(count(node_cpu_seconds_total{instance="host-0",job="node_exporter"}) by (cpu))`
	start := time.Now()
	res, err := client.PostQueryRange(ctx, &promapi.QueryRangeForm{
		Query: q,
		Start: prometheusTimestamp(start.Add(-time.Minute)),
		End:   prometheusTimestamp(start),
		Step:  promapi.NewOptString("1s"),
	})
	if err != nil {
		return nil, errors.Wrap(err, "query")
	}
	stats := &cpuQueryStats{
		Count:    -1,
		Duration: time.Since(start),
	}
	matrix, ok := res.Data.GetMatrix()
	if !ok {
		return nil, errors.Errorf("unexpected response type %q", res.Data.Type)
	}
	for _, result := range matrix.Result {
		for _, point := range result.Values {
			if stats.Count == -1 {
				stats.Count = int(point.V)
			}
			if point.V == 0 || stats.Count != int(point.V) {
				return nil, errors.Errorf("unexpected value %f", point.V)
			}
			ts := time.Unix(int64(point.T), 0)
			if ts.After(stats.Latest) {
				stats.Latest = ts
			}
		}
	}
	return stats, nil
}

func (s *Bench) RunQueryReporter(ctx context.Context) error {
	client, err := promapi.NewClient(s.queryAddr)
	if err != nil {
		return errors.Wrap(err, "create prometheus client")
	}

	ticker := time.NewTicker(s.queryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			stats, err := s.runQueryCPU(ctx, client)
			var b strings.Builder
			b.WriteString("query: ")
			if err != nil {
				b.WriteString(err.Error())
				continue
			}
			if stats.Count > 0 {
				b.WriteString(fmt.Sprintf("cpu=%s", fmtInt(stats.Count)))
			} else {
				b.WriteString("cpu=N/A")
			}
			b.WriteString(" ")
			b.WriteString(fmt.Sprintf("d=%s", stats.Duration.Round(time.Millisecond)))
			if !stats.Latest.IsZero() {
				b.WriteString(" ")
				b.WriteString(fmt.Sprintf("lag=%s", time.Since(stats.Latest).Round(time.Millisecond)))
			}
			fmt.Println(b.String())
		}
	}
}

func newBenchCommand() *cobra.Command {
	var b Bench
	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Start remote write benchmark",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := b.parseTargets(args); err != nil {
				return err
			}
			return b.run(cmd.Context())
		},
	}
	f := cmd.Flags()
	f.StringVar(&b.nodeExporterAddr, "nodeExporterAddr", "", "address for node exporter to listen")
	f.StringVar(&b.addr, "addr", "", "address to listen")
	f.StringVar(&b.agentAddr, "agentAddr", "", "address for vmagent to listen")
	f.IntVar(&b.targetsCount, "targetsCount", 100, "The number of scrape targets to return from -httpListenAddr. Each target has the same address defined by -targetAddr")
	f.DurationVar(&b.scrapeInterval, "scrapeInterval", time.Second*5, "The scrape_interval to set at the scrape config returned from -httpListenAddr")
	f.DurationVar(&b.pollExporterInterval, "pollExporterInterval", time.Second, "Interval to poll the node exporter filling up cache")
	f.DurationVar(&b.scrapeConfigUpdateInterval, "scrapeConfigUpdateInterval", time.Minute*10, "The -scrapeConfigUpdatePercent scrape targets are updated in the scrape config returned from -httpListenAddr every -scrapeConfigUpdateInterval")
	f.Float64Var(&b.scrapeConfigUpdatePercent, "scrapeConfigUpdatePercent", 1, "The -scrapeConfigUpdatePercent scrape targets are updated in the scrape config returned from -httpListenAddr ever -scrapeConfigUpdateInterval")
	f.StringVar(&b.clickhouseAddr, "clickhouseAddr", "", "clickhouse tcp protocol addr to get actual stats from")
	f.StringVar(&b.queryAddr, "queryAddr", "", "addr to query PromQL from")
	f.DurationVar(&b.queryInterval, "queryInterval", time.Second*5, "interval to query PromQL")
	f.BoolVar(&b.useVictoria, "useVictoria", true, "use vmagent instead of prometheus")
	return cmd
}
