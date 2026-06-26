package profileql

import (
	"regexp"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/lexerql"
)

// Label is a ProfileQL label name.
type Label string

const (
	// LabelName is the label holding the profile-type selector, equivalent to
	// Prometheus' __name__ metric name label.
	LabelName = "__name__"
	// LabelServiceName is the conventional service name label.
	LabelServiceName = "service_name"
	// LabelProfileType is the internal label holding the profile type.
	LabelProfileType = "__profile_type__"
)

// String implements [fmt.Stringer].
func (l Label) String() string {
	return string(l)
}

// IsValidLabel validates a label name.
func IsValidLabel[S ~string | ~[]byte](s S) error {
	if len(s) == 0 {
		return errors.New("label name cannot be empty")
	}
	if r := s[0]; !lexerql.IsIdentStartRune(r) {
		return errors.Errorf("invalid label name character %[1]q (%[1]U) at 0", r)
	}
	for i, r := range string(s) {
		if lexerql.IsIdentRune(r) || r == '.' {
			continue
		}
		return errors.Errorf("invalid label name character %[1]q (%[1]U) at %[2]d", r, i)
	}
	return nil
}

func compileLabelRegex(re string) (*regexp.Regexp, error) {
	return regexp.Compile("^(?:" + re + ")$")
}
