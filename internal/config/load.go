// Package config holds the configuration blocks shared by oteldb binaries.
//
// Each binary defines its own root config struct embedding the blocks for the roles it plays, so a
// field added to a block reaches every binary at once. Blocks carry their own json/yaml tags and
// [Defaulter] implementation; when defaults are applied is up to the binary.
package config

import (
	"os"
	"path/filepath"

	"github.com/go-faster/yaml"
)

// Defaulter fills zero fields of a config block with their defaults.
type Defaulter interface {
	SetDefaults()
}

// LoadOptions configures [Load].
type LoadOptions struct {
	// Fallback is the config path used when the requested one is empty.
	Fallback string
	// Optional makes a missing Fallback file yield a zero config instead of an error. It only
	// applies to Fallback: an explicitly requested path must exist.
	Optional bool
}

// Load reads a YAML config file into C.
//
// Defaults are not applied: the caller decides whether they run before or after its own
// environment overrides.
func Load[C any](name string, opts LoadOptions) (cfg C, _ error) {
	if name == "" {
		name = opts.Fallback
		if opts.Optional {
			if _, err := os.Stat(name); err != nil {
				return cfg, nil
			}
		}
	}

	data, err := os.ReadFile(filepath.Clean(name))
	if err != nil {
		return cfg, err
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}
