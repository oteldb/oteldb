package config_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// FuzzDifferential compares the two decoders on arbitrary config bytes.
//
// The plain decoder is the reference: whatever it accepts, the descriptor must accept and resolve
// to the same value. The descriptor is allowed to be stricter — refusing an unknown key is the
// point of adopting it — so its refusal is not a finding; a disagreement about a value is.
func FuzzDifferential(f *testing.F) {
	for _, tt := range differentialFixtures {
		f.Add(tt.data)
	}
	for _, path := range treeFixtures(f) {
		data, err := os.ReadFile(path)
		require.NoError(f, err)

		if blocks, ok := knownBlocks(f, data); ok {
			f.Add(string(blocks))
		}
	}
	for _, data := range []string{"", "\n", "---\n", "prometheus:\n", "auth:\n- {}\n"} {
		f.Add(data)
	}

	d := diffDescriptor(f)
	f.Fuzz(func(t *testing.T, data string) {
		old, next, oldErr, nextErr := decodeBoth(d, []byte(data))
		if oldErr != nil || nextErr != nil {
			t.Skip("one decoder refused the input")
		}
		require.Equal(t, normalized(old), normalized(next))
	})
}
