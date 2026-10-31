package otlpdirect

import "github.com/oteldb/storage/otlp/otlpdecode"

// LogsConverter is the storage library's native logs decoder.
type LogsConverter = otlpdecode.LogsConverter

// MetricsConverter is the storage library's native metrics decoder.
type MetricsConverter = otlpdecode.MetricsConverter

// TracesConverter is the storage library's native traces decoder.
type TracesConverter = otlpdecode.TracesConverter

// ProfilesConverter is the storage library's native profiles decoder.
type ProfilesConverter = otlpdecode.ProfilesConverter

// Dropped counts unrepresentable metric points and exemplars.
type Dropped = otlpdecode.Dropped
