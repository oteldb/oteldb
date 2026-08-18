package config

import (
	"github.com/go-faster/figureout"

	"github.com/oteldb/oteldb/internal/httpmiddleware"
)

// The describe functions below register a block's fields against the schema of whatever root
// embeds it. They take the root's schema rather than a schema of their own because figureout binds
// by pointer address inside the root, which is also what keeps the block's spelling flat: a
// [Listener] embedded with `,inline` contributes bind and auth as siblings of the block's own
// fields, and that is exactly what registering them at the same level produces.
//
// A block is nested under its own key by wrapping the call in [figureout.Group]:
//
//	figureout.Group(s, "prometheus", func(s *figureout.Schema[Config]) {
//		config.DescribePrometheus(s, &c.Prometheus)
//	})

// DescribeListener registers the listener fields every API block embeds.
func DescribeListener[R any](s *figureout.Schema[R], cfg *Listener) {
	figureout.Value(s, &cfg.Bind, "bind")
	figureout.ListOf(s, &cfg.Auth, "auth", DescribeAuth)
}

// DescribeAuth describes one authentication entry.
func DescribeAuth(cfg *Auth, s *figureout.Schema[Auth]) {
	figureout.Value(s, &cfg.Type, "type")
	figureout.ListOf(s, &cfg.Tokens, "tokens", describeToken)
	figureout.ListOf(s, &cfg.Users, "users", describeUser)
}

func describeToken(cfg *httpmiddleware.Token, s *figureout.Schema[httpmiddleware.Token]) {
	figureout.Value(s, &cfg.Token, "token", figureout.Secret())
	figureout.Value(s, &cfg.TokenFile, "token_file")
}

func describeUser(cfg *httpmiddleware.UserCredentials, s *figureout.Schema[httpmiddleware.UserCredentials]) {
	figureout.Value(s, &cfg.User, "user")
	figureout.Value(s, &cfg.Password, "password", figureout.Secret())
	figureout.Value(s, &cfg.PasswordFile, "password_file")
}

// DescribePrometheus registers the Prometheus API block.
func DescribePrometheus[R any](s *figureout.Schema[R], cfg *Prometheus) {
	DescribeListener(s, &cfg.Listener)
	figureout.Value(s, &cfg.MaxSamples, "max_samples")
	figureout.Value(s, &cfg.MaxTimeseries, "max_timeseries")
	figureout.Value(s, &cfg.Timeout, "timeout")
	figureout.Value(s, &cfg.LookbackDelta, "lookback_delta")
	figureout.Value(s, &cfg.EnableAtModifier, "enable_at_modifier")
	figureout.Value(s, &cfg.EnableNegativeOffset, "enable_negative_offset")
	figureout.Value(s, &cfg.EnablePerStepStats, "enable_per_step_stats")
	figureout.Value(s, &cfg.EnableScarecrowEngine, "enable_scarecrow_engine")
	figureout.Value(s, &cfg.DisableRateOffloading, "disable_rate_offloading")
	figureout.Value(s, &cfg.DisableMetricOffloading, "disable_metric_offloading")
	figureout.Group(s, "cache", func(s *figureout.Schema[R]) {
		figureout.Value(s, &cfg.Cache.MaxBytes, "max_bytes")
		figureout.Value(s, &cfg.Cache.SafetyLag, "safety_lag")
	})
}

// DescribeLoki registers the Loki API block.
func DescribeLoki[R any](s *figureout.Schema[R], cfg *Loki) {
	DescribeListener(s, &cfg.Listener)
	figureout.Value(s, &cfg.DrilldownEnabled, "drilldown_enabled")
	figureout.Value(s, &cfg.LookbackDelta, "lookback_delta")
	figureout.Value(s, &cfg.MaxSampleRows, "max_sample_rows")
	figureout.Value(s, &cfg.MaxSampleResultBytes, "max_sample_result_bytes")
}

// DescribeTempo registers the Tempo API block.
func DescribeTempo[R any](s *figureout.Schema[R], cfg *Tempo) {
	DescribeListener(s, &cfg.Listener)
}

// DescribePyroscope registers the Pyroscope API block.
func DescribePyroscope[R any](s *figureout.Schema[R], cfg *Pyroscope) {
	DescribeListener(s, &cfg.Listener)
}

// DescribeAdmin registers the admin API block.
func DescribeAdmin[R any](s *figureout.Schema[R], cfg *Admin) {
	DescribeListener(s, &cfg.Listener)
}

// DescribeHealthCheck registers the health check block.
func DescribeHealthCheck[R any](s *figureout.Schema[R], cfg *HealthCheck) {
	DescribeListener(s, &cfg.Listener)
}

// DescribeCluster registers the storage cluster block a stateless node reads.
func DescribeCluster[R any](s *figureout.Schema[R], cfg *Cluster) {
	figureout.Value(s, &cfg.Etcd, "etcd")
	figureout.Value(s, &cfg.Root, "root")
	figureout.Value(s, &cfg.RF, "rf")
	figureout.Value(s, &cfg.ShardsPerTenant, "shards_per_tenant")
	figureout.Value(s, &cfg.DialTimeout, "dial_timeout")
}
