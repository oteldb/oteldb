package config_test

import (
	"encoding/json"
	"testing"

	"github.com/go-faster/figureout/schema/jsonschema"
	"github.com/stretchr/testify/require"
)

// TestJSONSchema is oteldb#1286: the same declaration that decodes the config also describes it,
// which is what a chart's values.schema.json would be generated from.
func TestJSONSchema(t *testing.T) {
	t.Parallel()

	raw, diags, err := jsonschema.Generate(diffDescriptor(t), jsonschema.Semantic())
	require.NoError(t, err)
	require.Empty(t, diags, "every block must be representable")

	var doc struct {
		Properties map[string]struct {
			Properties map[string]json.RawMessage `json:"properties"`
		} `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(raw, &doc))

	require.Contains(t, doc.Properties, "prometheus")
	require.Contains(t, doc.Properties["prometheus"].Properties, "max_samples")
	require.Contains(t, doc.Properties["prometheus"].Properties, "bind",
		"the listener stays flat in the schema, as it is in a config file")
	require.NotContains(t, doc.Properties["prometheus"].Properties, "listener")
	require.Contains(t, doc.Properties["cluster"].Properties, "shards_per_tenant")
}
