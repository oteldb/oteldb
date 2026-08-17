package promrw

import (
	"bytes"
	"unicode/utf8"

	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/prompb"
)

// A series whose labels cannot be stored is skipped and counted rather than failing the request:
// one bad series in a batch of thousands must not cost the sender the whole batch, and since the
// sender would retry the same bytes forever, failing would also wedge its queue. Prometheus'
// receiver does the same.
//
// The rules are Prometheus' under UTF-8 validation, which is what its own remote write handler
// applies: a metric name is required, every label name is non-empty, every name and value is valid
// UTF-8, and no label name repeats. The last one matters most here — the engine hashes the
// attribute set to derive a series id, and a duplicated key would make that id depend on the order
// the sender happened to write two labels in.

// Counts is what a conversion did with a request's points, in the sender's terms: one native
// histogram counts once however many classic series it decomposed into, because one is what the
// sender sent and one is what it will resend if told this did not land.
type Counts struct {
	// Samples is the number of float samples ingested.
	Samples int
	// Histograms is the number of native histograms ingested.
	Histograms int
	// Exemplars is the number of exemplars ingested, which is always zero: the engine has nowhere
	// to put them. It is reported rather than omitted so a sender is not left guessing.
	Exemplars int
	// Rejected is what the request carried but the conversion did not ingest.
	Rejected Rejected
}

// Rejected counts what a conversion did not ingest, by reason.
type Rejected struct {
	// Old is the number of points and histograms older than [Options.TimeThreshold].
	Old int
	// Invalid is the number of points and histograms whose series has unstorable labels.
	Invalid int
	// Unsupported is the number of histograms carrying no decomposable bucket layout.
	Unsupported int
}

// Total is the number of points rejected for any reason.
func (r Rejected) Total() int { return r.Old + r.Invalid + r.Unsupported }

// add sums two counts, so a per-request total can accumulate per-series ones.
func (r *Rejected) add(o Rejected) {
	r.Old += o.Old
	r.Invalid += o.Invalid
	r.Unsupported += o.Unsupported
}

// validLabels reports whether the series' labels can be stored. Duplicates are not checked here:
// they are only detectable once the set is sorted, which [Converter.labelAttrs] does.
func validLabels(labels []prompb.Label) bool {
	names := 0
	for _, l := range labels {
		if len(l.Name) == 0 || !utf8.Valid(l.Name) || !utf8.Valid(l.Value) {
			return false
		}
		if bytes.Equal(l.Name, nameLabel) {
			// An empty metric name is no name at all.
			if len(l.Value) == 0 {
				return false
			}
			names++
		}
	}

	// Exactly one name: none leaves the series unaddressable, and two is a duplicate label whose
	// resolution would come down to which one the reader looked at first.
	return names == 1
}

// hasDuplicateKey reports whether a sorted attribute set repeats a key.
func hasDuplicateKey(attrs signal.Attributes) bool {
	for i := 1; i < len(attrs); i++ {
		if bytes.Equal(attrs[i-1].Key, attrs[i].Key) {
			return true
		}
	}

	return false
}
