package logqlabels

import (
	"regexp"

	"github.com/cespare/xxhash/v2"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/lokiapi"
)

// EmptyAggregatedLabels returns empty set of aggregated labels.
func EmptyAggregatedLabels() AggregatedLabels {
	return emptyAggregatedLabels
}

var (
	emptyAggregatedLabels = new(emptyLabels)

	zeroHash = xxhash.New().Sum64()
)

type emptyLabels struct{}

func (l *emptyLabels) By(...logql.Label) AggregatedLabels { return l }

func (l *emptyLabels) Without(...logql.Label) AggregatedLabels { return l }

func (l *emptyLabels) Key() GroupingKey { return zeroHash }

func (l *emptyLabels) Replace(_, _, _ string, _ *regexp.Regexp) AggregatedLabels { return l }

func (l *emptyLabels) AsLokiAPI() lokiapi.LabelSet { return lokiapi.LabelSet{} }
