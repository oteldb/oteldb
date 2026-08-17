package config

// Tempo is Tempo API config.
type Tempo struct {
	Bind string `json:"bind" yaml:"bind"`
	Auth []Auth `json:"auth" yaml:"auth"`
}

// SetDefaults implements [Defaulter].
func (cfg *Tempo) SetDefaults() {
	if cfg.Bind == "" {
		cfg.Bind = ":3200"
	}
}

// Pyroscope is Pyroscope API config.
type Pyroscope struct {
	Bind string `json:"bind" yaml:"bind"`
	Auth []Auth `json:"auth" yaml:"auth"`
}

// SetDefaults implements [Defaulter].
func (cfg *Pyroscope) SetDefaults() {
	if cfg.Bind == "" {
		cfg.Bind = ":4040"
	}
}

// Admin is the admin panel API config.
type Admin struct {
	Bind string `json:"bind" yaml:"bind"`
	Auth []Auth `json:"auth" yaml:"auth"`
}

// SetDefaults implements [Defaulter].
func (cfg *Admin) SetDefaults() {
	if cfg.Bind == "" {
		cfg.Bind = ":8090"
	}
}

// HealthCheck is health check config.
type HealthCheck struct {
	Bind string `json:"bind" yaml:"bind"`
	Auth []Auth `json:"auth" yaml:"auth"`
}

// SetDefaults implements [Defaulter].
func (cfg *HealthCheck) SetDefaults() {
	if cfg.Bind == "" {
		cfg.Bind = ":13133"
	}
}
