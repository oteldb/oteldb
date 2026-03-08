package oteldbexporter

import (
	"go.opentelemetry.io/collector/pdata/plog"
)

// sampleLogRecords removes log records that the sampler decides to drop,
// pruning empty ScopeLogs and ResourceLogs afterwards.
func sampleLogRecords(ld plog.Logs, cfg SamplingConfig) {
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
			sl.LogRecords().RemoveIf(func(plog.LogRecord) bool { return !cfg.keep() })
			return sl.LogRecords().Len() == 0
		})
		return rl.ScopeLogs().Len() == 0
	})
}
