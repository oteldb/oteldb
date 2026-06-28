package lokicompliance

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/lokiapi"
)

func matrix(series ...lokiapi.Series) lokiapi.QueryResponseData {
	var d lokiapi.QueryResponseData
	d.SetMatrixResult(lokiapi.MatrixResult{Result: series})
	return d
}

func seriesWith(labels map[string]string) lokiapi.Series {
	return lokiapi.Series{
		Metric: lokiapi.NewOptLabelSet(lokiapi.LabelSet(labels)),
		Values: []lokiapi.FPoint{{T: 1, V: "5"}},
	}
}

// TestDropDiscoveryLabels verifies that results differing only by the implementation-defined
// discovery labels compare equal after normalization — the behavior the compliance comparison
// relies on so an engine that synthesizes service_name/detected_level is not penalized.
func TestDropDiscoveryLabels(t *testing.T) {
	// Reference (Loki, discovery disabled): just the real stream label.
	ref := matrix(seriesWith(map[string]string{"job": "varlogs"}))
	// Test engine: the same series plus synthesized discovery labels.
	got := matrix(seriesWith(map[string]string{
		"job":            "varlogs",
		"service_name":   "unknown_service",
		"detected_level": "ERROR",
		"level":          "ERROR",
	}))

	// Before stripping they differ.
	require.NotEmpty(t, cmp.Diff(ref, got), "expected a diff before normalization")

	dropDiscoveryLabels(&ref)
	dropDiscoveryLabels(&got)
	sortResponse(&ref)
	sortResponse(&got)

	require.Empty(t, cmp.Diff(ref, got), "discovery-only difference must normalize away")
	// The real label survives.
	require.Equal(t, "varlogs", got.MatrixResult.Result[0].Metric.Value["job"])
}
