package storagebackup

import (
	"context"
	"encoding/json"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// Store is the read seam a backup needs: the per-signal fetchers, the query budget, and the
// in-memory snapshot that enumerates tenants and their retained window. *[storage.Storage]
// implements it.
type Store interface {
	Fetcher(tenants ...signal.TenantID) fetch.Fetcher
	LogFetcher(tenants ...signal.TenantID) fetch.Fetcher
	TraceFetcher(tenants ...signal.TenantID) fetch.Fetcher
	WithQueryBudget(ctx context.Context) context.Context
	Inspect() storage.StoreStats
}

var _ Store = (*storage.Storage)(nil)

// BackupSignals are the signals a backup covers, in a stable order. Profiles are excluded; see the
// package documentation.
var BackupSignals = []signal.Signal{signal.Log, signal.Trace, signal.Metric}

// BackupOptions configures a [Backup].
type BackupOptions struct {
	// Signals selects what to back up. Empty ⇒ [BackupSignals].
	Signals []signal.Signal
	// From and To bound the scan (To exclusive). A zero From starts at the oldest retained data; a
	// zero To ends at now minus Lag.
	From, To time.Time
	// Lag keeps the window behind the ingest edge, so the newest day is not scanned while it is
	// still filling. Ignored when To is set. Zero ⇒ [DefaultLag].
	Lag time.Duration
	// Resume skips a (tenant, signal, day) whose file already exists, making an interrupted run
	// restartable. Off by default so a plain re-run rewrites the window.
	Resume bool
	// MaxChunkBytes is the size a fetch batch is split at, bounding the encoder's buffer. Zero ⇒
	// [DefaultMaxChunkBytes]; a larger value than a reader accepts is clamped down to it.
	MaxChunkBytes int
	// Now overrides the clock, for tests.
	Now func() time.Time
}

// DefaultLag is the default [BackupOptions.Lag]: an hour behind the ingest edge.
const DefaultLag = time.Hour

// BackupStats counts what a backup wrote.
type BackupStats struct {
	Files   int
	Skipped int
	Streams int
	Chunks  int
	Rows    int
}

// Backup copies a storage engine's data into a backup directory. See the package documentation for
// the layout and the fidelity contract.
type Backup struct {
	store Store
	lg    *zap.Logger
	opts  BackupOptions
}

// NewBackup creates a [Backup] over store.
func NewBackup(store Store, lg *zap.Logger, opts BackupOptions) *Backup {
	if len(opts.Signals) == 0 {
		opts.Signals = BackupSignals
	}
	if opts.Lag == 0 {
		opts.Lag = DefaultLag
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	opts.MaxChunkBytes = clampChunkLimit(opts.MaxChunkBytes)
	return &Backup{store: store, lg: lg, opts: opts}
}

// tenantWindow is one logical tenant's shard keys and retained window for one signal.
type tenantWindow struct {
	shards     []signal.TenantID
	mint, maxt int64
}

// Create writes the backup into dir.
func (b *Backup) Create(ctx context.Context, dir string) (BackupStats, error) {
	var stats BackupStats

	if err := os.MkdirAll(dir, 0o750); err != nil {
		return stats, errors.Wrap(err, "create backup directory")
	}

	manifest := Manifest{Version: FormatVersion, CreatedAt: b.opts.Now().UTC()}
	for _, sig := range b.opts.Signals {
		tenants := b.tenants(sig)
		for _, tenant := range slices.Sorted(maps.Keys(tenants)) {
			w := tenants[tenant]
			from, to, ok := b.window(w)
			if !ok {
				b.lg.Info("Nothing to back up",
					zap.String("signal", sig.String()),
					zap.String("tenant", string(tenant)),
				)
				continue
			}
			if manifest.Start.IsZero() || from.Before(manifest.Start) {
				manifest.Start = from
			}
			if to.After(manifest.End) {
				manifest.End = to
			}

			for day := from; day.Before(to); day = day.AddDate(0, 0, 1) {
				info, skipped, err := b.day(ctx, dir, sig, tenant, w.shards, day)
				if err != nil {
					return stats, errors.Wrapf(err, "back up %s/%s %s", sig, tenant, day.Format(dayLayout))
				}
				if skipped {
					stats.Skipped++
					continue
				}
				if info == nil {
					continue
				}
				manifest.Files = append(manifest.Files, *info)
				stats.Files++
				stats.Streams += info.Streams
				stats.Chunks += info.Chunks
				stats.Rows += info.Rows
			}
		}
	}

	if err := writeManifest(dir, manifest); err != nil {
		return stats, err
	}
	return stats, nil
}

// day writes one (tenant, signal, UTC day) file, returning nil when the day held no data. The
// second result reports a day skipped because its file already exists.
func (b *Backup) day(
	ctx context.Context,
	dir string,
	sig signal.Signal,
	tenant signal.TenantID,
	shards []signal.TenantID,
	day time.Time,
) (_ *FileInfo, skipped bool, rerr error) {
	rel := filePath(sig, tenant, day.Format(dayLayout))
	if b.opts.Resume {
		if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(rel))); err == nil {
			return nil, true, nil
		}
	}

	// The window is inclusive on both ends at the fetch seam, so the day ends one nanosecond
	// before midnight — otherwise a record exactly at midnight lands in two adjacent files.
	start := day.UnixNano()
	end := day.AddDate(0, 0, 1).UnixNano() - 1

	w, err := createChunkWriter(dir, rel, FileHeader{
		Version: FormatVersion,
		Signal:  sig.String(),
		Tenant:  string(tenant),
		Day:     day.Format(dayLayout),
		Start:   start,
		End:     end,
	}, b.opts.MaxChunkBytes)
	if err != nil {
		return nil, false, err
	}
	defer func() {
		if rerr != nil {
			w.Abort()
		}
	}()

	ctx = b.store.WithQueryBudget(ctx)
	for _, shard := range shards {
		if err := b.scan(ctx, w, sig, shard, start, end); err != nil {
			return nil, false, errors.Wrapf(err, "scan shard %q", shard)
		}
	}

	// An empty day is not worth a file: it would only make a resumed run believe it had already
	// done work it had not, and it clutters the tree for a sparse tenant.
	if w.rows == 0 {
		w.Abort()
		return nil, false, nil
	}

	info := &FileInfo{
		Path:    rel,
		Signal:  sig.String(),
		Tenant:  string(tenant),
		Day:     day.Format(dayLayout),
		Streams: w.streams,
		Chunks:  w.chunks,
		Rows:    w.rows,
	}
	if err := w.Close(); err != nil {
		return nil, false, err
	}
	b.lg.Info("Backed up",
		zap.String("signal", sig.String()),
		zap.String("tenant", string(tenant)),
		zap.String("day", info.Day),
		zap.Int("streams", info.Streams),
		zap.Int("chunks", info.Chunks),
		zap.Int("rows", info.Rows),
	)
	return info, false, nil
}

// scan fetches every stream of one shard key in the window and writes it as chunks.
func (b *Backup) scan(
	ctx context.Context,
	w *chunkWriter,
	sig signal.Signal,
	shard signal.TenantID,
	start, end int64,
) error {
	// No matchers, no conditions and no projection: the fetch seam reads that as every stream, every
	// record and every column of the signal's schema.
	req := fetch.Request{
		Tenant: shard,
		Signal: sig,
		Start:  start,
		End:    end,
		Scope:  fetch.NewScope(),
	}

	var fetcher fetch.Fetcher
	switch sig {
	case signal.Log:
		fetcher = b.store.LogFetcher(shard)
	case signal.Trace:
		fetcher = b.store.TraceFetcher(shard)
	case signal.Metric:
		fetcher = b.store.Fetcher(shard)
	default:
		return errors.Errorf("signal %s is not backed up", sig)
	}

	it, err := fetcher.Fetch(ctx, req)
	if err != nil {
		return errors.Wrap(err, "fetch")
	}
	defer func() {
		_ = it.Close()
	}()

	var weighted int
	for {
		batch, err := it.Next(ctx)
		switch {
		case errors.Is(err, io.EOF):
			if weighted > 0 {
				// The ingest model has no field for a sample's lossy-sampling weight, so a restore
				// of these samples under-counts. Say so rather than let it pass silently.
				b.lg.Warn("Lossy-sampling scale factors are not preserved by a backup",
					zap.String("tenant", string(shard)),
					zap.Int("series", weighted),
				)
			}
			return nil
		case err != nil:
			return errors.Wrap(err, "next batch")
		}
		if len(batch.Timestamps) == 0 {
			continue
		}
		if batch.ScaleFactors != nil {
			weighted++
		}
		if err := w.Write(&Chunk{
			Series:     batch.Series,
			Timestamps: batch.Timestamps,
			Values:     batch.Values,
			Columns:    batch.Columns,
		}); err != nil {
			return errors.Wrap(err, "write chunk")
		}
	}
}

// tenants groups the store's tenants for one signal by their *logical* tenant, folding a cluster's
// shard keys ("default/_s0") back into the tenant they split ("default").
//
// This is the step that makes a backup re-shardable: the shard key is a function of the source
// cluster's shards_per_tenant, so recording it would pin the data to a cluster of that exact shape.
// Recording the logical tenant instead lets the destination's write path derive its own.
func (b *Backup) tenants(sig signal.Signal) map[signal.TenantID]*tenantWindow {
	out := map[signal.TenantID]*tenantWindow{}
	for _, t := range b.store.Inspect().Tenants {
		for _, s := range t.Signals {
			if s.Signal != sig {
				continue
			}
			if s.HasReadGap {
				b.lg.Warn("Tenant has a read gap; the backup will be incomplete over it",
					zap.String("tenant", string(t.Tenant)),
					zap.String("signal", sig.String()),
					zap.Int64("gap_after_unix_nano", s.ReadGapAfterUnixNano),
				)
			}

			logical := cluster.TenantOfShard(t.Tenant)
			w, ok := out[logical]
			if !ok {
				w = &tenantWindow{mint: s.MinTimeUnixNano, maxt: s.MaxTimeUnixNano}
				out[logical] = w
			}
			w.shards = append(w.shards, t.Tenant)
			if s.MinTimeUnixNano != 0 && (w.mint == 0 || s.MinTimeUnixNano < w.mint) {
				w.mint = s.MinTimeUnixNano
			}
			if s.MaxTimeUnixNano > w.maxt {
				w.maxt = s.MaxTimeUnixNano
			}
		}
	}
	for _, w := range out {
		slices.Sort(w.shards)
	}
	return out
}

// window resolves the UTC-day-aligned scan window for a tenant, intersecting its retained range
// with the configured bounds. It reports false when nothing is left to scan.
func (b *Backup) window(w *tenantWindow) (from, to time.Time, ok bool) {
	if w.maxt == 0 {
		return time.Time{}, time.Time{}, false
	}

	first := time.Unix(0, w.mint).UTC()
	if !b.opts.From.IsZero() && b.opts.From.After(first) {
		first = b.opts.From.UTC()
	}

	// The bounds below are inclusive instants, day-aligned only at the end: a To of exactly
	// midnight is exclusive, so it must not pull the following day into the scan.
	last := time.Unix(0, w.maxt).UTC()
	limit := b.opts.To.UTC()
	if b.opts.To.IsZero() {
		limit = b.opts.Now().UTC().Add(-b.opts.Lag)
	}
	if limit = limit.Add(-time.Nanosecond); limit.Before(last) {
		last = limit
	}

	if last.Before(first) {
		return time.Time{}, time.Time{}, false
	}
	return first.Truncate(24 * time.Hour), last.Truncate(24*time.Hour).AddDate(0, 0, 1), true
}

func writeManifest(dir string, m Manifest) error {
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return errors.Wrap(err, "encode manifest")
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), append(raw, '\n'), 0o600); err != nil {
		return errors.Wrap(err, "write manifest")
	}
	return nil
}
