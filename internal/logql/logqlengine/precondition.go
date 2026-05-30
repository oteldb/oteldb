package logqlengine

import (
	"github.com/oteldb/oteldb/internal/logql"
)

type queryConditions struct {
	prefilter Processor
	Labels    []logql.LabelMatcher
}

func extractQueryConditions(caps QuerierCapabilities, sel logql.Selector) (cond queryConditions, _ error) {
	var prefilters []Processor

	for _, lm := range sel.Matchers {
		// If storage supports the operation, just pass it as a parameter.
		if caps.Label.Supports(lm.Op) {
			cond.Labels = append(cond.Labels, lm)
			continue
		}

		// Otherwise, build a prefilter.
		proc, err := buildLabelMatcher(lm)
		if err != nil {
			return cond, err
		}
		prefilters = append(prefilters, proc)
	}

	switch len(prefilters) {
	case 0:
		cond.prefilter = NopProcessor
	case 1:
		cond.prefilter = prefilters[0]
	default:
		cond.prefilter = &Pipeline{Stages: prefilters}
	}
	return cond, nil
}
