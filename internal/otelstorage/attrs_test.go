package otelstorage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var keyToLabelTests = []struct {
	key  string
	want string
}{
	{"", ""},
	{"foo", "foo"},
	{"f_oo", "f_oo"},

	{"0foo", "_0foo"},
	{"foo.bar", "foo_bar"},
	{"foo/bar", "foo_bar"},
	{"receiver/accepted_spans/0", "receiver_accepted_spans_0"},
	{"a🐹/b🐹/0", "a__b__0"},
}

func TestKeyToLabel(t *testing.T) {
	for i, tt := range keyToLabelTests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			require.Equal(t, tt.want, KeyToLabel(tt.key))
			require.Equal(t, tt.want, string(AppendKeyToLabel(nil, tt.key)))
		})
	}
}

func TestKeyToLabelAllocs(t *testing.T) {
	var sink string
	for _, tt := range keyToLabelTests {
		gotAllocs := testing.AllocsPerRun(1000, func() {
			sink = KeyToLabel(tt.key)
		})
		require.LessOrEqual(t, gotAllocs, 1.0)
		if tt.key != "" && sink == "" {
			t.Fail()
		}
	}
}

func TestAppendKeyToLabelAllocs(t *testing.T) {
	buf := make([]byte, 0, 256)
	for _, tt := range keyToLabelTests {
		gotAllocs := testing.AllocsPerRun(1000, func() {
			buf = AppendKeyToLabel(buf, tt.key)
		})
		require.Zero(t, gotAllocs)
		if tt.key != "" && len(buf) == 0 {
			t.Fail()
		}
	}
}

func BenchmarkKeyToLabel(b *testing.B) {
	b.ReportAllocs()

	var sink string
	for i := 0; i < b.N; i++ {
		sink = KeyToLabel("receiver/accepted_spans/0")
	}
	if sink == "" {
		b.Fatal()
	}
}
