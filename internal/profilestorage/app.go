package profilestorage

import "github.com/oteldb/oteldb/internal/profileql"

// Units describes the unit of a profile's sample values.
type Units string

// Known profile sample units, matching the Pyroscope set.
const (
	UnitsSamples         Units = "samples"
	UnitsObjects         Units = "objects"
	UnitsGoroutines      Units = "goroutines"
	UnitsBytes           Units = "bytes"
	UnitsLockNanoseconds Units = "lock_nanoseconds"
	UnitsLockSamples     Units = "lock_samples"
)

// App is the metadata of a profiled application, as returned by the Pyroscope
// `/api/apps` endpoint.
type App struct {
	// Name is the application name, typically "<service>.<profile-type>".
	Name string
	// SpyName is the profiler ("spy") that produced the profile, if known.
	SpyName string
	// SampleRate is the sampling rate in Hz, if known.
	SampleRate uint32
	// Units is the unit of the profile's sample values.
	Units Units
}

// UnitsForProfileType returns the conventional [Units] for a profile type,
// following Pyroscope's mapping from sample type to unit.
func UnitsForProfileType(t profileql.ProfileType) Units {
	switch t.SampleType {
	case "inuse_objects", "alloc_objects", "goroutine", "samples":
		return UnitsObjects
	case "cpu":
		return UnitsSamples
	}
	switch Units(t.SampleUnit) {
	case UnitsSamples, UnitsObjects, UnitsGoroutines, UnitsBytes, UnitsLockNanoseconds, UnitsLockSamples:
		return Units(t.SampleUnit)
	default:
		return UnitsSamples
	}
}
