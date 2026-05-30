// Package logqlabels contains LogQL label utilities.
package logqlabels

import (
	"regexp"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/lokiapi"
)

// GroupingKey is a key to group metrics by label.
type GroupingKey = uint64

// AggregatedLabels is a set of labels.
type AggregatedLabels interface {
	// By returns new set of labels containing only given list of labels.
	By(...logql.Label) AggregatedLabels
	// Without returns new set of labels without given list of labels.
	Without(...logql.Label) AggregatedLabels
	// Key computes grouping key from set of labels.
	Key() GroupingKey
	// Replace replaces labels using given regexp.
	Replace(dstLabel, replacement, srcLabel string, re *regexp.Regexp) AggregatedLabels

	// AsLokiAPI returns API structure for label set.
	AsLokiAPI() lokiapi.LabelSet
}
