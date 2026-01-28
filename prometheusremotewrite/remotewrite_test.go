package prometheusremotewrite

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestDecodeRequest(t *testing.T) {
	raw := readTestData(t, "reqs-1k-zstd.rwq")

	series, err := DecodeRequest(bytes.NewReader(raw), Settings{TimeThreshold: 1_000_000 * time.Hour})
	require.NoError(t, err)

	var jm pmetric.JSONMarshaler
	jsonData, err := jm.MarshalMetrics(series)
	require.NoError(t, err)
	dst := new(bytes.Buffer)
	require.NoError(t, json.Indent(dst, jsonData, "", " "))
	gold.Str(t, dst.String(), "request.json")
}
