// Package storagebackup implements backup and restore for the embedded storage engine
// ([github.com/oteldb/storage]), the backend that replaces chstorage.
//
// # Why restore goes through the write path
//
// A backup is a stream of logical telemetry — streams, records, series and samples — not a copy of
// the engine's part files. Restore rebuilds the data by calling the engine's ordinary Write* entry
// points, so every write-time decision is taken again against the *destination's* configuration:
// tenant routing, and, in cluster mode, the shard key and its ring placement. That is what makes
// one tool cover three jobs instead of only the first:
//
//   - disaster recovery,
//   - re-sharding: raising cluster.shards_per_tenant re-keys every shard
//     ([github.com/oteldb/storage/cluster.ShardKeyOf] turns tenant "default" into "default/_s0" …
//     "default/_sN-1"), which strands data written under the old keys. A backup taken before the
//     change and restored after it lands under the new keys.
//   - moving a tenant between clusters.
//
// A byte-for-byte part copy would solve only disaster recovery, and only into an identically
// shaped cluster.
//
// # On-disk layout
//
//	<dir>/manifest.json
//	<dir>/<signal>/<tenant>/<YYYY-MM-DD>.obk.zst
//
// Signal is the stable [github.com/oteldb/storage/signal.Signal] name ("log", "trace", "metric").
// The tenant path element is percent-escaped; the authoritative tenant id is the one in each
// file's header, not the one in the path. One file holds one UTC day of one (tenant, signal),
// which is the same day-at-a-time slicing chstorage's backup uses.
//
// A file is a zstd stream of: the magic, a JSON header, then length-delimited chunks. See [Chunk].
//
// A chunk is a stream (or metric series) identity plus a run of its rows. A fetch batch is one
// stream's whole day, which on a busy tenant is hundreds of megabytes, so a batch larger than
// [BackupOptions.MaxChunkBytes] is split by row over as many chunks as it needs, each repeating
// the identity. Every chunk therefore decodes on its own, and restore sees a split batch as
// several writes of the same stream — the shape live ingest already produces when one stream's
// records arrive in more than one export.
//
// manifest.json is written last and is informational — it records the window, the files and their
// row counts for an operator. Restore walks the tree and trusts each file's own header, so a
// backup whose manifest is missing still restores.
//
// # Backup is restartable, and does not require stopping ingest
//
// A day file is written to "<name>.tmp" and renamed into place only after its last chunk is
// flushed, so a file that exists is complete. With [BackupOptions.Resume] an existing file is
// skipped, which makes an interrupted run restartable at (tenant, signal, day) granularity — the
// same granularity ch2storagebackend's checkpoint journal uses. There is no mid-file resume.
//
// The scan reads through the ordinary fetch seam, so concurrent ingest is safe and no flush or
// downtime is needed. It does mean the newest day is a moving target: records that arrive after
// that day was scanned are not in the backup. Use [BackupOptions.Lag] (or an explicit To) to keep
// the window behind the ingest edge.
//
// # Backup does not write to the data directory
//
// [EngineConfig.ReadOnly] — which odbbackup always sets — opens the engine with everything that
// writes turned off: no WAL recovery, no flush, no merges, no retention, and no cluster membership.
// Without it, opening a data directory is a write: recovery replays the WAL into a head, the close
// that follows flushes that head into a new part, and the WAL is checkpointed, discarding
// segments. Against a running node that makes the backup a second writer, discarding segments the
// node still owns — "back up" would mean "modify".
//
// The cost is the unflushed head: a read-only open sees the flushed parts only, and whatever the
// engine has ingested but not yet flushed is not backed up (it logs a warning when the WAL is
// non-empty). With the default [BackupOptions.Lag] that data is outside the window anyway, since
// the head holds the newest writes; keep Lag at or above the engine's flush interval.
//
// A restore is the other way round: it writes, so it needs the destination to itself. Do not point
// odbrestore at a running node's data directory — restore into a stopped node, then start it.
//
// # Fidelity contract
//
// A backup can only carry what the engine stores. What the engine drops at ingest is already gone
// before a backup runs; the contract below is stated against the engine's stored state, and is
// exercised by the round-trip test.
//
// Round-trips exactly:
//
//   - Stream identity: resource (attributes and schema URL), instrumentation scope (name, version,
//     schema URL, attributes), with attribute types preserved.
//   - Logs: every column of [github.com/oteldb/storage/signal/log.Schema] — timestamp, observed
//     timestamp, severity number and text, body, trace and span id, flags, dropped count, and the
//     record attributes.
//   - Traces: timestamp (span start), duration (hence end), kind, status code and message, trace,
//     span and parent span ids, name, span attributes, events and links.
//   - Metrics: series identity including the reserved __name__/__unit__/__kind__/
//     __temporality__/__monotonic__ labels and the data-point attributes, plus every (timestamp,
//     value) sample. Histograms are already decomposed into _count/_sum/_bucket{le} series at
//     ingest, so they restore as the same decomposed series.
//
// Approximated:
//
//   - Span structural ids (nested_set_left/right, parent_id) are derived per write batch, not
//     stored as source data. Restore recomputes them from the spans in each restored batch, so a
//     trace whose spans fall in different batches is reconstructed per batch. This is the same
//     approximation live ingest already makes when a trace's spans arrive in separate exports.
//   - Data lands in the destination's tenant, which oteldb derives from its own configuration
//     rather than from the backup. Backing up a cluster's shard keys ("default/_s0") records the
//     logical tenant ("default"); restoring several logical tenants into a single-tenant
//     destination merges them, and [RestoreOptions.Tenant] is the way to avoid that.
//
// Lost:
//
//   - Metric sample start timestamps. The engine has a StartTs field on the ingest model but never
//     persists it, so it is not readable and is restored as zero.
//   - Lossy-sampling scale factors. A sample kept by budgeted sampling carries the weight of the
//     samples it stands for; the ingest model has no field for that weight, so a restored sample
//     weighs one. Backup logs a warning when it sees weighted samples.
//   - Span trace state, flags and dropped-attribute counts, and any field outside the engine's
//     schemas. These are dropped at ingest, before any backup sees them.
//
// # Not covered
//
// Profiles are not backed up. A profile row stores only a content-addressed stack id into a
// per-tenant symbol side store; rebuilding an ingestible
// [github.com/oteldb/storage/signal/profile.Profiles] means re-interning a dictionary out of
// resolved frames, which loses mappings, addresses and the original string table. That deserves
// its own export rather than a lossy approximation folded in here. The format is keyed by signal
// name, so adding "profile" later needs no format change.
package storagebackup
