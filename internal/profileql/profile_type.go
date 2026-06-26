package profileql

import (
	"strings"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/lexerql"
)

// ProfileType is a parsed profile-type selector.
//
// The textual form is
//
//	<name>:<sample_type>:<sample_unit>:<period_type>:<period_unit>[:delta]
//
// for example "process_cpu:cpu:nanoseconds:cpu:nanoseconds".
type ProfileType struct {
	// Name is the profile name, e.g. "process_cpu" or "memory".
	Name string
	// SampleType is the sample value type, e.g. "cpu" or "alloc_space".
	SampleType string
	// SampleUnit is the sample value unit, e.g. "nanoseconds" or "bytes".
	SampleUnit string
	// PeriodType is the period type, e.g. "cpu".
	PeriodType string
	// PeriodUnit is the period unit, e.g. "nanoseconds".
	PeriodUnit string
	// Delta denotes that the profile carries delta values.
	Delta bool
}

// ParseProfileType parses a profile-type selector string.
func ParseProfileType(id string) (ProfileType, error) {
	parts := strings.Split(id, ":")
	if len(parts) != 5 && len(parts) != 6 {
		return ProfileType{}, errors.Errorf(
			"profile-type selector must be of the form <name>:<sample-type>:<sample-unit>:<period-type>:<period-unit>(:delta), got %d parts: %q",
			len(parts), id,
		)
	}
	for i, p := range parts[:5] {
		if err := validateTypePart(p); err != nil {
			return ProfileType{}, errors.Wrapf(err, "profile-type selector part %d (%q)", i, id)
		}
	}
	pt := ProfileType{
		Name:       parts[0],
		SampleType: parts[1],
		SampleUnit: parts[2],
		PeriodType: parts[3],
		PeriodUnit: parts[4],
	}
	if len(parts) == 6 {
		if parts[5] != "delta" {
			return ProfileType{}, errors.Errorf("profile-type selector suffix must be \"delta\", got %q", parts[5])
		}
		pt.Delta = true
	}
	return pt, nil
}

// validateTypePart ensures a profile-type part is a non-empty,
// Prometheus-compatible identifier. This guarantees the textual form lexes back
// as a single bare token, keeping [ParseProfileType] and the parser consistent.
func validateTypePart(p string) error {
	if p == "" {
		return errors.New("is empty")
	}
	if r := p[0]; !lexerql.IsIdentStartRune(r) {
		return errors.Errorf("invalid leading character %[1]q (%[1]U)", r)
	}
	for i, r := range p {
		if !lexerql.IsIdentRune(r) {
			return errors.Errorf("invalid character %[1]q (%[1]U) at %[2]d", r, i)
		}
	}
	return nil
}

// ID returns the canonical 5-part profile-type identifier, without the delta
// suffix. This is the value stored in the [LabelProfileType] label.
func (t ProfileType) ID() string {
	return strings.Join([]string{
		t.Name,
		t.SampleType,
		t.SampleUnit,
		t.PeriodType,
		t.PeriodUnit,
	}, ":")
}

// String implements [fmt.Stringer]. It reproduces the parsed form, including
// the delta suffix when set.
func (t ProfileType) String() string {
	id := t.ID()
	if t.Delta {
		return id + ":delta"
	}
	return id
}
