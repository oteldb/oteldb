package odbsafety

import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator"

// NewBuilder creates a new stanza operator builder.
func NewBuilder() operator.Builder {
	return NewConfig()
}

// Register registers the odbsafety operator in a stanza operator registry.
func Register(registry *operator.Registry) {
	registry.Register(Type, NewBuilder)
}

// RegisterDefault registers the odbsafety operator in the default stanza registry.
func RegisterDefault() {
	operator.Register(Type, NewBuilder)
}
