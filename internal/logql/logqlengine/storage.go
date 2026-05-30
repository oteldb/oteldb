package logqlengine

import (
	"context"

	"github.com/oteldb/oteldb/internal/logql"
)

// SupportedOps is a bitset defining ops supported by Querier.
type SupportedOps uint64

// Add sets capability.
func (caps *SupportedOps) Add(ops ...logql.BinOp) {
	for _, op := range ops {
		*caps |= SupportedOps(1 << op)
	}
}

// Supports checks if storage supports given ops.
func (caps SupportedOps) Supports(op logql.BinOp) bool {
	mask := SupportedOps(1 << op)
	return caps&mask != 0
}

// QuerierCapabilities defines what operations storage can do.
type QuerierCapabilities struct {
	Label SupportedOps
	Line  SupportedOps
}

// Querier does queries to storage.
type Querier interface {
	// Capabilities returns Querier capabilities.
	//
	// NOTE: engine would call once and then save value.
	// 	Capabilities should not change over time.
	Capabilities() QuerierCapabilities
	// Query creates new [PipelineNode].
	Query(ctx context.Context, selector []logql.LabelMatcher) (PipelineNode, error)
}
