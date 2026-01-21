// Copyright  The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prometheusremotewritereceiver

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

func TestLoadConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	typ := component.MustNewType(typeStr)
	tests := []struct {
		id       component.ID
		expected component.Config
	}{
		{
			id:       component.NewIDWithName(typ, "defaults"),
			expected: createDefaultConfig(),
		},
		{
			id: component.NewIDWithName(typ, ""),
			expected: &Config{
				ServerConfig: confighttp.ServerConfig{
					Endpoint:           "0.0.0.0:19291",
					TLS:                configoptional.None[configtls.ServerConfig](),
					CORS:               configoptional.None[confighttp.CORSConfig](),
					Auth:               configoptional.None[confighttp.AuthConfig](),
					MaxRequestBodySize: 0,
					IncludeMetadata:    false,
				},
				TimeThreshold: 24 * time.Hour,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(&cfg))
			assert.Equal(t, tt.expected, cfg)
		})
	}
}
