// Package internal contains go:generate annotations.
package internal

//go:generate go tool ogen --target tempoapi     --package tempoapi     ../_oas/tempo.yml
//go:generate go tool ogen --target lokiapi      --package lokiapi      ../_oas/loki.yml
//go:generate go tool ogen --target promapi      --package promapi      ../_oas/prometheus.yml
//go:generate go tool ogen --target pyroscopeapi --package pyroscopeapi ../_oas/pyroscope.yml
//go:generate go tool ogen --target sentryapi    --package sentryapi    ../_oas/sentry.yml

//go:generate go tool ogen --target otelbotapi   --package otelbotapi   ../_oas/otelbot.yml
