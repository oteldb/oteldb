package config

// Listener is the HTTP listener every API block is served on. Blocks embed it inline, so it is
// spelled the same in a config file as if each block declared the fields itself, while letting a
// binary range over its listeners regardless of which API each one serves.
//
// The `,inline` tags on the embedding blocks are load-bearing: without them the fields nest under
// a "listener" key in YAML. [TestListenerShape] pins that.
type Listener struct {
	Bind string `json:"bind" yaml:"bind"`
	Auth []Auth `json:"auth" yaml:"auth"`
}

// setDefaults applies bind as the listen address when none is configured.
func (cfg *Listener) setDefaults(bind string) {
	if cfg.Bind == "" {
		cfg.Bind = bind
	}
}

// Tempo is Tempo API config.
type Tempo struct {
	Listener `json:",inline" yaml:",inline"`
}

// SetDefaults implements [Defaulter].
func (cfg *Tempo) SetDefaults() {
	cfg.Listener.setDefaults(":3200")
}

// Pyroscope is Pyroscope API config.
type Pyroscope struct {
	Listener `json:",inline" yaml:",inline"`
}

// SetDefaults implements [Defaulter].
func (cfg *Pyroscope) SetDefaults() {
	cfg.Listener.setDefaults(":4040")
}

// Admin is the admin panel API config.
type Admin struct {
	Listener `json:",inline" yaml:",inline"`
}

// SetDefaults implements [Defaulter].
func (cfg *Admin) SetDefaults() {
	cfg.Listener.setDefaults(":8090")
}

// HealthCheck is health check config.
type HealthCheck struct {
	Listener `json:",inline" yaml:",inline"`
}

// SetDefaults implements [Defaulter].
func (cfg *HealthCheck) SetDefaults() {
	cfg.Listener.setDefaults(":13133")
}
