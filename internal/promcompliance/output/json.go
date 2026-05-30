package output

import (
	"encoding/json"
	"fmt"

	"github.com/oteldb/oteldb/internal/promcompliance/comparer"
	"github.com/oteldb/oteldb/internal/promcompliance/config"
)

// JSONResult is the JSON output format.
type JSONResult struct {
	TotalResults   int                  `json:"totalResults"`
	Results        []*comparer.Result   `json:"results,omitempty"`
	IncludePassing bool                 `json:"includePassing"`
	QueryTweaks    []*config.QueryTweak `json:"queryTweaks,omitempty"`
}

// JSON produces JSON-based output for a number of query results.
func JSON(results []*comparer.Result, includePassing bool, tweaks []*config.QueryTweak) {
	buf, err := json.Marshal(JSONResult{
		TotalResults:   len(results),
		Results:        results,
		IncludePassing: includePassing,
		QueryTweaks:    tweaks,
	})
	if err != nil {
		panic(err)
	}
	fmt.Print(string(buf))
}
