package clusteradmin

import "github.com/oteldb/oteldb/internal/adminapi"

type retentionAdmissionState struct {
	mode           adminapi.OptBool
	missing, mixed bool
}

func (s *retentionAdmissionState) add(mode adminapi.OptBool) {
	value, known := mode.Get()
	if !known {
		s.missing = true
		return
	}
	if prior, have := s.mode.Get(); have && prior != value {
		s.mixed = true
	}
	s.mode = mode
}

func (s retentionAdmissionState) result() adminapi.OptBool {
	if s.missing || s.mixed {
		return adminapi.OptBool{}
	}
	return s.mode
}

func addRetentionStats(into *adminapi.EngineSignalStats, from adminapi.EngineSignalStats) {
	cutoff, owner := from.RetainFrom.Get()
	current, have := into.RetainFrom.Get()
	if owner && (!have || cutoff.After(current) || cutoff.Equal(current) && preferRetention(&from, into)) {
		into.RetainFrom = from.RetainFrom
		into.RetentionBoundBy = from.RetentionBoundBy
		into.RetentionPolicy = from.RetentionPolicy
	} else if !owner && !have && !into.RetentionPolicy.IsSet() {
		into.RetentionPolicy = from.RetentionPolicy
	}
	if lag, known := from.RetentionLagSeconds.Get(); owner && known {
		if previous, had := into.RetentionLagSeconds.Get(); !had || lag > previous {
			into.RetentionLagSeconds = from.RetentionLagSeconds
		}
	}
}

func preferRetention(from, into *adminapi.EngineSignalStats) bool {
	rank := func(bound adminapi.RetentionBound) int {
		switch bound {
		case adminapi.RetentionBoundAge:
			return 3
		case adminapi.RetentionBoundSignalBytes:
			return 2
		case adminapi.RetentionBoundPooledBytes:
			return 1
		default:
			return 0
		}
	}
	a, b := rank(from.RetentionBoundBy.Or(adminapi.RetentionBoundNone)), rank(into.RetentionBoundBy.Or(adminapi.RetentionBoundNone))
	return a > b || a == b && from.RetentionPolicy.IsSet() && !into.RetentionPolicy.IsSet()
}
