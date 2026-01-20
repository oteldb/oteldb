package prometheusremotewrite

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_metricSuffixes(t *testing.T) {
	tests := []struct {
		input  string
		wantS1 string
		wantS2 string
	}{
		{"test", "", ""},
		{"test_value51", "", ""},
		{"test_value61_count", "value61", "count"},
		{"test_value81_bytes", "value81", "bytes"},
		{"test_value81_count_bytes", "count", "bytes"},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			gotS1, gotS2 := metricSuffixes(tt.input)
			require.Equal(t, tt.wantS1, gotS1)
			require.Equal(t, tt.wantS2, gotS2)
		})
	}
}

func TestIsValidCumulativeSuffix(t *testing.T) {
	type args struct {
		suffix string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "sum",
			args: args{
				suffix: "sum",
			},
			want: true,
		},
		{
			name: "count",
			args: args{
				suffix: "count",
			},
			want: true,
		},
		{
			name: "total",
			args: args{
				suffix: "total",
			},
			want: true,
		},
		{
			name: "foo",
			args: args{
				suffix: "bar",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, IsValidCumulativeSuffix(tt.args.suffix), "IsValidCumulativeSuffix(%v)", tt.args.suffix)
		})
	}
}

func TestIsValidSuffix(t *testing.T) {
	type args struct {
		suffix string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "max",
			args: args{
				suffix: "max",
			},
			want: true,
		},
		{
			name: "sum",
			args: args{
				suffix: "sum",
			},
			want: true,
		},
		{
			name: "count",
			args: args{
				suffix: "count",
			},
			want: true,
		},
		{
			name: "total",
			args: args{
				suffix: "total",
			},
			want: true,
		},
		{
			name: "foo",
			args: args{
				suffix: "bar",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, IsValidSuffix(tt.args.suffix), "IsValidSuffix(%v)", tt.args.suffix)
		})
	}
}

func TestIsValidUnit(t *testing.T) {
	type args struct {
		unit string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "seconds",
			args: args{
				unit: "seconds",
			},
			want: true,
		},
		{
			name: "bytes",
			args: args{
				unit: "bytes",
			},
			want: true,
		},
		{
			name: "foo",
			args: args{
				unit: "bar",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, IsValidUnit(tt.args.unit), "IsValidUnit(%v)", tt.args.unit)
		})
	}
}
