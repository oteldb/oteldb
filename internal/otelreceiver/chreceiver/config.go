package chreceiver

import (
	"net/url"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/chotel"
)

const defaultPollRate = 500 * time.Millisecond

// Config defines config for [Receiver].
type Config struct {
	DSN      string              `mapstructure:"dsn"`
	PollRate time.Duration       `mapstructure:"poll_rate"`
	Lag      time.Duration       `mapstructure:"lag"`
	Lookback time.Duration       `mapstructure:"lookback"`
	Filter   chotel.FilterConfig `mapstructure:"filter"`
}

// Validate validates receiver config.
func (c *Config) Validate() error {
	if c.DSN == "" {
		return errors.New("'dsn' is required")
	}
	if _, err := url.Parse(c.DSN); err != nil {
		return errors.Wrap(err, "parse dsn")
	}
	if c.PollRate <= 0 {
		return errors.New("'poll_rate' must be positive")
	}
	if c.Lag <= 0 {
		return errors.New("'lag' must be positive")
	}
	if c.Lookback <= 0 {
		return errors.New("'lookback' must be positive")
	}
	return nil
}
