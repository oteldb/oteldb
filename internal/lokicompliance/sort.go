package lokicompliance

import (
	"cmp"
	"slices"

	"github.com/oteldb/oteldb/internal/lokiapi"
)

// discoveryLabels are produced by attribute discovery (service-name detection, log-level detection),
// which Loki documents as implementation-defined and which the compliance reference disables
// (discover_service_name: [], discover_log_levels: false). A conformant engine may still synthesize
// them, so they are stripped from both the reference and test results before comparison — comparing
// them would penalize an implementation-defined choice. Safe here because no compliance query selects
// or groups by these labels.
var discoveryLabels = []string{"service_name", "detected_level", "level"}

// dropDiscoveryLabels removes the implementation-defined discovery labels from every series/stream of
// a response, in place.
func dropDiscoveryLabels(data *lokiapi.QueryResponseData) {
	strip := func(ls lokiapi.LabelSet) {
		for _, k := range discoveryLabels {
			delete(ls, k)
		}
	}
	switch data.Type {
	case lokiapi.StreamsResultQueryResponseData:
		for _, s := range data.StreamsResult.Result {
			strip(s.Stream.Value)
		}
	case lokiapi.VectorResultQueryResponseData:
		for _, s := range data.VectorResult.Result {
			strip(s.Metric.Value)
		}
	case lokiapi.MatrixResultQueryResponseData:
		for _, s := range data.MatrixResult.Result {
			strip(s.Metric.Value)
		}
	}
}

func sortResponse(data *lokiapi.QueryResponseData) {
	switch data.Type {
	case lokiapi.StreamsResultQueryResponseData:
		slices.SortFunc(data.StreamsResult.Result, func(a, b lokiapi.Stream) int {
			return compareLabelSets(a.Stream.Value, b.Stream.Value)
		})
	case lokiapi.VectorResultQueryResponseData:
		slices.SortFunc(data.VectorResult.Result, func(a, b lokiapi.Sample) int {
			return cmp.Compare(a.Value.T, b.Value.T)
		})
	case lokiapi.MatrixResultQueryResponseData:
		slices.SortFunc(data.MatrixResult.Result, func(a, b lokiapi.Series) int {
			return compareLabelSets(a.Metric.Value, b.Metric.Value)
		})
	}
}

// compareLabelSets orders two labelsets.
func compareLabelSets(a, b lokiapi.LabelSet) int {
	if c := cmp.Compare(len(a), len(b)); c != 0 {
		return c
	}

	strs := make([]string, 0, len(a)+len(b))
	// Compare by keys.
	{
		for key := range a {
			strs = append(strs, key)
		}
		astrs := strs
		slices.Sort(astrs)

		for key := range b {
			strs = append(strs, key)
		}
		bstrs := strs[len(astrs):]
		slices.Sort(bstrs)

		if c := slices.Compare(astrs, bstrs); c != 0 {
			return c
		}
	}

	strs = strs[:0]
	// Compare by values.
	{
		for _, value := range a {
			strs = append(strs, value)
		}
		astrs := strs
		slices.Sort(astrs)

		for _, value := range b {
			strs = append(strs, value)
		}
		bstrs := strs[len(astrs):]
		slices.Sort(bstrs)

		if c := slices.Compare(astrs, bstrs); c != 0 {
			return c
		}
	}
	return 0
}
