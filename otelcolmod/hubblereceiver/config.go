// Package hubblereceiver implements an OpenTelemetry Collector receiver that
// streams flows from Hubble Relay via gRPC and emits OTLP logs.
package hubblereceiver

import (
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/config/configgrpc"
)

// Config defines config for [Receiver].
type Config struct {
	configgrpc.ClientConfig `mapstructure:",squash"`

	ClusterID   int64  `mapstructure:"cluster_id"`
	ClusterName string `mapstructure:"cluster_name"`
}

// Validate validates receiver config.
func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return errors.New("endpoint is required")
	}
	return nil
}
