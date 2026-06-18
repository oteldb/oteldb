package tetragonreceiver

import (
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/config/configgrpc"
)

// Config defines config for [Receiver].
// ClusterName comes from the Tetragon stream, not from config.
type Config struct {
	configgrpc.ClientConfig `mapstructure:",squash"`

	// ClusterID is a numeric cluster identifier set on every log record as
	// "tetragon.cluster.id".
	ClusterID int64 `mapstructure:"cluster_id"`

	// DisableEventDescription leaves the log body empty when true.
	DisableEventDescription bool `mapstructure:"disable_event_description"`
}

// Validate validates receiver config.
func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return errors.New("endpoint is required")
	}
	return nil
}
