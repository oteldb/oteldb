package hareceiver

import (
	"path"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
)

// SourceKind selects a Home Assistant log stream.
type SourceKind string

// Supported [SourceKind] values.
//
// Everything except addon maps to a Supervisor plugin or component that Home
// Assistant Core allowlists at /api/hassio/<kind>/logs. Which of them exist
// depends on the installation: cli and observer are absent on some.
const (
	SourceKindHost       SourceKind = "host"
	SourceKindCore       SourceKind = "core"
	SourceKindSupervisor SourceKind = "supervisor"
	SourceKindDNS        SourceKind = "dns"
	SourceKindAudio      SourceKind = "audio"
	SourceKindMulticast  SourceKind = "multicast"
	SourceKindCLI        SourceKind = "cli"
	SourceKindObserver   SourceKind = "observer"
	SourceKindAddon      SourceKind = "addon"
)

// Source is a single log stream to ingest.
type Source struct {
	Kind SourceKind `mapstructure:"kind"`

	// Addon is the add-on slug, required when Kind is [SourceKindAddon].
	Addon string `mapstructure:"addon"`
}

// Name returns a stable human-readable identifier of the source.
func (s Source) Name() string {
	if s.Kind == SourceKindAddon {
		return string(s.Kind) + "/" + s.Addon
	}
	return string(s.Kind)
}

// Path returns the Supervisor API path of the log stream, relative to the
// Home Assistant instance root.
func (s Source) Path() string {
	if s.Kind == SourceKindAddon {
		return path.Join("api/hassio/addons", s.Addon, "logs")
	}
	return path.Join("api/hassio", string(s.Kind), "logs")
}

// StorageKey returns the storage extension key holding the source cursor.
func (s Source) StorageKey() string {
	return "cursor/" + s.Name()
}

func (s Source) validate() error {
	switch s.Kind {
	case SourceKindHost, SourceKindCore, SourceKindSupervisor,
		SourceKindDNS, SourceKindAudio, SourceKindMulticast,
		SourceKindCLI, SourceKindObserver:
		if s.Addon != "" {
			return errors.Errorf("addon is only allowed for %q kind", SourceKindAddon)
		}
		return nil
	case SourceKindAddon:
		if s.Addon == "" {
			return errors.Errorf("addon is required for %q kind", SourceKindAddon)
		}
		return nil
	case "":
		return errors.New("kind is required")
	default:
		return errors.Errorf("unknown kind %q", s.Kind)
	}
}

// Config defines config for [Receiver].
type Config struct {
	confighttp.ClientConfig `mapstructure:",squash"`

	// Token is a long-lived access token belonging to an admin user.
	Token configopaque.String `mapstructure:"token"`

	// Sources selects which log streams to ingest.
	Sources []Source `mapstructure:"sources"`

	// PollInterval between cursor-advancing requests.
	PollInterval time.Duration `mapstructure:"poll_interval"`

	// BatchSize limits how many journal entries a single request may return.
	BatchSize int `mapstructure:"batch_size"`

	// ParseMessage extracts the level, thread and logger that Core and
	// Supervisor embed in the message, and reduces the body to the message
	// itself. Enabled by default; nothing is lost, since the prefix is fully
	// described by the resulting fields.
	ParseMessage bool `mapstructure:"parse_message"`

	// RecombineWindow is how long a fragment of a multi-line message may lag
	// the previous one and still be joined to it. Zero disables recombination
	// and emits one record per journal entry.
	RecombineWindow time.Duration `mapstructure:"recombine_window"`

	// SeverityFromMessage enables best-effort severity detection for messages
	// that ParseMessage does not recognize, such as host logs. Home Assistant
	// does not expose the journal PRIORITY field through its API, see the
	// package README.
	SeverityFromMessage bool `mapstructure:"severity_from_message"`

	// StorageID references a storage extension used to persist cursors across
	// restarts. Without it the receiver starts at the tail on every start.
	StorageID *component.ID `mapstructure:"storage"`
}

// Validate validates receiver config.
func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return errors.New("endpoint is required")
	}
	if c.Token == "" {
		return errors.New("token is required")
	}
	if len(c.Sources) == 0 {
		return errors.New("at least one source is required")
	}
	seen := make(map[string]struct{}, len(c.Sources))
	for i, s := range c.Sources {
		if err := s.validate(); err != nil {
			return errors.Wrapf(err, "source %d", i)
		}
		name := s.Name()
		if _, ok := seen[name]; ok {
			return errors.Errorf("duplicate source %q", name)
		}
		seen[name] = struct{}{}
	}
	if c.PollInterval <= 0 {
		return errors.New("poll_interval must be positive")
	}
	if c.BatchSize <= 0 {
		return errors.New("batch_size must be positive")
	}
	if c.RecombineWindow < 0 {
		return errors.New("recombine_window must not be negative")
	}
	return nil
}
