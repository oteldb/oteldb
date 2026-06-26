package profileql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type profileTypeTestCase struct {
	input   string
	want    ProfileType
	wantErr bool
}

var profileTypeTests = []profileTypeTestCase{
	{
		"process_cpu:cpu:nanoseconds:cpu:nanoseconds",
		ProfileType{
			Name:       "process_cpu",
			SampleType: "cpu",
			SampleUnit: "nanoseconds",
			PeriodType: "cpu",
			PeriodUnit: "nanoseconds",
		},
		false,
	},
	{
		"memory:alloc_space:bytes:space:bytes:delta",
		ProfileType{
			Name:       "memory",
			SampleType: "alloc_space",
			SampleUnit: "bytes",
			PeriodType: "space",
			PeriodUnit: "bytes",
			Delta:      true,
		},
		false,
	},
	{
		"mutex:contentions:count:mutex:count",
		ProfileType{
			Name:       "mutex",
			SampleType: "contentions",
			SampleUnit: "count",
			PeriodType: "mutex",
			PeriodUnit: "count",
		},
		false,
	},
	// Too few parts.
	{"process_cpu:cpu:nanoseconds:cpu", ProfileType{}, true},
	// Too many parts.
	{"a:b:c:d:e:f:g", ProfileType{}, true},
	// Bad delta suffix.
	{"a:b:c:d:e:notdelta", ProfileType{}, true},
	// Part is not a valid identifier.
	{"0:0:0:0:0", ProfileType{}, true},
	{"a:b:c:d:1e", ProfileType{}, true},
	{"a:b:c.d:e:f", ProfileType{}, true},
	{"a b:c:d:e:f", ProfileType{}, true},
	// Empty parts.
	{"a::c:d:e", ProfileType{}, true},
	{":b:c:d:e", ProfileType{}, true},
	{"a:b:c:d:", ProfileType{}, true},
	{"", ProfileType{}, true},
	// Empty delta part.
	{"a:b:c:d:e:", ProfileType{}, true},
}

func TestParseProfileType(t *testing.T) {
	for _, tt := range profileTypeTests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseProfileType(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)

			// String round-trips back to the input.
			require.Equal(t, tt.input, got.String())
		})
	}
}

func TestProfileTypeID(t *testing.T) {
	pt := ProfileType{
		Name:       "memory",
		SampleType: "alloc_space",
		SampleUnit: "bytes",
		PeriodType: "space",
		PeriodUnit: "bytes",
		Delta:      true,
	}
	require.Equal(t, "memory:alloc_space:bytes:space:bytes", pt.ID())
	require.Equal(t, "memory:alloc_space:bytes:space:bytes:delta", pt.String())
}

func FuzzParseProfileType(f *testing.F) {
	for _, tt := range profileTypeTests {
		f.Add(tt.input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		pt, err := ParseProfileType(input)
		if err != nil {
			return
		}
		// A parsed profile type must round-trip through String back to a value
		// that parses identically.
		reparsed, err := ParseProfileType(pt.String())
		if err != nil {
			t.Fatalf("re-parsing %q (from input %q) failed: %v", pt.String(), input, err)
		}
		if reparsed != pt {
			t.Fatalf("unstable round-trip: %+v != %+v (input %q)", reparsed, pt, input)
		}
	})
}
