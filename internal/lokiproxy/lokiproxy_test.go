package lokiproxy

import (
	"embed"
	"fmt"
	"path"
	"testing"

	"github.com/go-faster/jx"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/lokiapi"
)

//go:embed _testdata
var testdata embed.FS

func TestQueryResponseJSON(t *testing.T) {
	const root = "_testdata"
	resps, err := testdata.ReadDir(root)
	require.NoError(t, err)

	for _, respType := range resps {
		if !respType.IsDir() {
			continue
		}

		respType := respType.Name()
		t.Run(respType, func(t *testing.T) {
			files, err := testdata.ReadDir(path.Join(root, respType))
			require.NoError(t, err)

			for i, f := range files {
				t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
					data, err := testdata.ReadFile(path.Join(root, respType, f.Name()))
					require.NoError(t, err)

					var resp lokiapi.QueryResponse
					require.NoError(t, resp.Decode(jx.DecodeBytes(data)))

					var e jx.Encoder
					resp.Encode(&e)

					require.JSONEq(t, string(data), e.String())
				})
			}
		})
	}
}
