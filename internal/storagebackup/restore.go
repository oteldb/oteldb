package storagebackup

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/oteldb/storage/signal"
	siglog "github.com/oteldb/storage/signal/log"
	sigmetric "github.com/oteldb/storage/signal/metric"
	sigtrace "github.com/oteldb/storage/signal/trace"
)

// Sink is the write seam a restore ingests through: the storage engine's ordinary batch entry
// points. *[github.com/oteldb/oteldb/internal/storagebackend.Backend] implements it.
//
// Restore deliberately has no way to place part files, and no way to name a destination tenant:
// both are decisions the destination's write path makes, and letting a backup dictate them is
// exactly what would pin the data to the shape of the cluster it came from.
type Sink interface {
	WriteLogs(ctx context.Context, batch siglog.Logs) error
	WriteTraces(ctx context.Context, batch sigtrace.Traces) error
	WriteMetrics(ctx context.Context, batch sigmetric.Metrics) error
}

// DefaultRestoreBatchSize is the default [RestoreOptions.BatchSize].
const DefaultRestoreBatchSize = 5_000

// RestoreOptions configures a [Restore].
type RestoreOptions struct {
	// Signals selects what to restore. Empty ⇒ every signal present in the backup.
	Signals []signal.Signal
	// Tenant restores only files backed up from this logical tenant. Empty ⇒ every tenant, which
	// merges them all into the destination's own tenant.
	Tenant signal.TenantID
	// BatchSize is the number of records, spans or samples buffered per write. Zero ⇒
	// [DefaultRestoreBatchSize].
	BatchSize int
}

// RestoreStats counts what a restore ingested.
type RestoreStats struct {
	Files   int
	Streams int
	Rows    int
	Batches int
}

// Restore re-ingests a backup directory through a [Sink].
type Restore struct {
	sink Sink
	lg   *zap.Logger
	opts RestoreOptions
}

// NewRestore creates a [Restore] writing into sink.
func NewRestore(sink Sink, lg *zap.Logger, opts RestoreOptions) *Restore {
	if opts.BatchSize <= 0 {
		opts.BatchSize = DefaultRestoreBatchSize
	}
	return &Restore{sink: sink, lg: lg, opts: opts}
}

// Restore walks dir and re-ingests every chunk file it holds. The manifest is not consulted: each
// file carries its own header, so a backup that lost its manifest still restores.
func (r *Restore) Restore(ctx context.Context, dir string) (RestoreStats, error) {
	var stats RestoreStats

	files, err := chunkFiles(dir)
	if err != nil {
		return stats, err
	}
	for _, name := range files {
		fileStats, err := r.file(ctx, name)
		if err != nil {
			return stats, errors.Wrapf(err, "restore %s", name)
		}
		if fileStats.Files == 0 {
			continue
		}
		stats.Files += fileStats.Files
		stats.Streams += fileStats.Streams
		stats.Rows += fileStats.Rows
		stats.Batches += fileStats.Batches
	}
	return stats, nil
}

// file restores one chunk file. A file filtered out by the options yields zero stats.
func (r *Restore) file(ctx context.Context, name string) (stats RestoreStats, rerr error) {
	cr, h, err := openChunkReader(name)
	if err != nil {
		return stats, err
	}
	defer func() {
		if err := cr.Close(); err != nil && rerr == nil {
			rerr = err
		}
	}()

	sig, err := signal.ParseSignal(h.Signal)
	if err != nil {
		return stats, err
	}
	if len(r.opts.Signals) > 0 && !slices.Contains(r.opts.Signals, sig) {
		return stats, nil
	}
	if r.opts.Tenant != "" && signal.TenantID(h.Tenant) != r.opts.Tenant {
		return stats, nil
	}

	acc := newAccumulator(r.sink, sig, r.opts.BatchSize)
	for {
		c, err := cr.Next()
		switch {
		case errors.Is(err, io.EOF):
			if err := acc.Flush(ctx); err != nil {
				return stats, err
			}
			stats.Files = 1
			stats.Streams += acc.streams
			stats.Rows += acc.rows
			stats.Batches += acc.batches
			r.lg.Info("Restored",
				zap.String("signal", h.Signal),
				zap.String("tenant", h.Tenant),
				zap.String("day", h.Day),
				zap.Int("streams", acc.streams),
				zap.Int("rows", acc.rows),
			)
			return stats, nil
		case err != nil:
			return stats, err
		}
		if err := acc.Add(ctx, &c); err != nil {
			return stats, err
		}
	}
}

// accumulator buffers converted chunks into one signal's ingest batch and writes it once it holds
// batchSize rows.
type accumulator struct {
	sink      Sink
	sig       signal.Signal
	batchSize int

	logs    siglog.Logs
	traces  sigtrace.Traces
	metrics sigmetric.Metrics

	pending int
	streams int
	rows    int
	batches int
}

func newAccumulator(sink Sink, sig signal.Signal, batchSize int) *accumulator {
	return &accumulator{sink: sink, sig: sig, batchSize: batchSize}
}

// Add converts one chunk into the pending batch, flushing first if the batch is already full.
func (a *accumulator) Add(ctx context.Context, c *Chunk) error {
	var n int
	switch a.sig {
	case signal.Log:
		n = appendLogs(&a.logs, c)
	case signal.Trace:
		n = appendTraces(&a.traces, c)
	case signal.Metric:
		var err error
		if n, err = appendMetrics(&a.metrics, c); err != nil {
			return errors.Wrap(err, "convert metric series")
		}
	default:
		return errors.Errorf("signal %s cannot be restored", a.sig)
	}

	a.streams++
	a.rows += n
	a.pending += n
	if a.pending >= a.batchSize {
		return a.Flush(ctx)
	}
	return nil
}

// Flush writes the pending batch, if any.
func (a *accumulator) Flush(ctx context.Context) error {
	if a.pending == 0 {
		return nil
	}

	var err error
	switch a.sig {
	case signal.Log:
		err = a.sink.WriteLogs(ctx, a.logs)
		a.logs.Reset()
	case signal.Trace:
		err = a.sink.WriteTraces(ctx, a.traces)
		a.traces.Reset()
	case signal.Metric:
		err = a.sink.WriteMetrics(ctx, a.metrics)
		a.metrics.Reset()
	}
	if err != nil {
		return errors.Wrapf(err, "write %s batch", a.sig)
	}

	a.pending = 0
	a.batches++
	return nil
}

// chunkFiles lists a backup's chunk files in a deterministic order. Anything else in the tree —
// the manifest, an interrupted ".tmp" file, an operator's notes — is ignored.
func chunkFiles(dir string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), fileExt) {
			return nil
		}
		out = append(out, path)
		return nil
	})
	switch {
	case os.IsNotExist(err):
		return nil, errors.Errorf("backup directory %q does not exist", dir)
	case err != nil:
		return nil, errors.Wrap(err, "walk backup directory")
	}

	slices.Sort(out)
	return out, nil
}
