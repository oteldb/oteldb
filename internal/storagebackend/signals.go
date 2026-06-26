package storagebackend

// The logs and profiles query interfaces both declare LabelNames/LabelValues with different
// signatures, so a single type cannot satisfy both. Each signal's read interface is therefore
// implemented by its own small wrapper over the shared [Backend] (and its single *storage.Storage),
// obtained via the accessors below. Metrics stay on [Backend] directly (its interface does not
// collide).

// LogQuerier adapts the storage engine to oteldb's logs query interfaces
// (logstorage.Querier and logqlengine.Querier).
type LogQuerier struct{ b *Backend }

// TraceQuerier adapts the storage engine to oteldb's traces query interfaces
// (tracestorage.Querier and traceqlengine.Querier).
type TraceQuerier struct{ b *Backend }

// ProfileQuerier adapts the storage engine to oteldb's profiles query interface
// (profilestorage.Querier).
type ProfileQuerier struct{ b *Backend }

// Logs returns the logs querier over the backend's storage engine.
func (b *Backend) Logs() *LogQuerier { return &LogQuerier{b: b} }

// Traces returns the traces querier over the backend's storage engine.
func (b *Backend) Traces() *TraceQuerier { return &TraceQuerier{b: b} }

// Profiles returns the profiles querier over the backend's storage engine.
func (b *Backend) Profiles() *ProfileQuerier { return &ProfileQuerier{b: b} }
