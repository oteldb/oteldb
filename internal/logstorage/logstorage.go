// Package logstorage defines storage structure for logs storage.
package logstorage

import (
	"context"
	"time"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
)

// Querier is a logs storage query interface.
type Querier interface {
	// LabelNames returns all available label names.
	LabelNames(ctx context.Context, opts LabelsOptions) ([]string, error)
	// LabelValues returns all available label values for given label.
	LabelValues(ctx context.Context, labelName string, opts LabelsOptions) (iterators.Iterator[Label], error)
	// Series returns all available log series.
	Series(ctx context.Context, opts SeriesOptions) (Series, error)
	// DetectedLabels returns cardinality of all detected labels.
	DetectedLabels(ctx context.Context, opts LabelsOptions) ([]DetectedLabel, error)
}

// LabelsOptions defines options for [Querier.LabelNames] and [Querier.LabelValues] methods.
type LabelsOptions struct {
	// Start defines time range for search.
	//
	// Querier ignores parameter, if it is zero.
	Start time.Time
	// End defines time range for search.
	//
	// Querier ignores parameter, if it is zero.
	End time.Time
	// Selector that selects the streams to match.
	Query logql.Selector
	// Limit sets maximum number of results.
	//
	// Querier ignores parameter, if it is zero.
	Limit int
}

// SeriesOptions defines options for [Querier.Series] method.
type SeriesOptions struct {
	// Start defines time range for search.
	//
	// Querier ignores parameter, if it is zero.
	Start time.Time
	// End defines time range for search.
	//
	// Querier ignores parameter, if it is zero.
	End time.Time
	// Selectors defines a list of matchers to filter series.
	Selectors []logql.Selector
}

// Inserter is a log storage insert interface.
type Inserter interface {
	// RecordWriter creates a new batch.
	RecordWriter(ctx context.Context) (RecordWriter, error)
}

// RecordWriter represents a log record batch.
type RecordWriter interface {
	// Add adds record to the batch.
	Add(record Record) error
	// Submit sends batch.
	Submit(ctx context.Context) error
	// Close frees resources.
	//
	// Callers should call [RecordWriter.Close] regardless if they called [RecordWriter.Submit] or not.
	Close() error
}
