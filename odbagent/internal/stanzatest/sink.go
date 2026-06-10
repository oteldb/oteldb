// Package stanzatest contains shared stanza test helpers for odbagent packages.
package stanzatest

import (
	"context"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/entry"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator"
	"go.uber.org/zap"
)

// Sink is a stanza operator that records received entries.
type Sink struct {
	entries []*entry.Entry
}

// NewSink creates a new sink operator.
func NewSink() *Sink {
	return &Sink{}
}

// Entries returns entries received by the sink.
func (s *Sink) Entries() []*entry.Entry {
	return s.entries
}

// Reset clears entries received by the sink.
func (s *Sink) Reset() {
	s.entries = s.entries[:0]
}

func (s *Sink) ID() string { return "sink" }

func (s *Sink) Type() string { return "sink" }

func (s *Sink) Start(operator.Persister) error { return nil }

func (s *Sink) Stop() error { return nil }

func (s *Sink) CanOutput() bool { return false }

func (s *Sink) Outputs() []operator.Operator { return nil }

func (s *Sink) GetOutputIDs() []string { return nil }

func (s *Sink) SetOutputs([]operator.Operator) error { return nil }

func (s *Sink) SetOutputIDs([]string) {}

func (s *Sink) CanProcess() bool { return true }

func (s *Sink) ProcessBatch(_ context.Context, entries []*entry.Entry) error {
	s.entries = append(s.entries, entries...)
	return nil
}

func (s *Sink) Process(_ context.Context, ent *entry.Entry) error {
	s.entries = append(s.entries, ent)
	return nil
}

func (s *Sink) Logger() *zap.Logger { return zap.NewNop() }
