package adminhandler

import (
	"testing"

	"github.com/oteldb/storage/signal"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/adminapi"
)

// Every storage signal must map onto a defined enum value: [mapSignal]'s fallback would otherwise
// emit a string that the generated client rejects while decoding another node's stats.
func TestMapSignal(t *testing.T) {
	for _, sig := range []signal.Signal{
		signal.Metric,
		signal.Log,
		signal.Trace,
		signal.Profile,
		signal.Exemplar,
	} {
		t.Run(sig.String(), func(t *testing.T) {
			got := mapSignal(sig)
			require.NoError(t, got.Validate())

			back, err := engineSignal(got)
			require.NoError(t, err)
			require.Equal(t, sig, back)
		})
	}
}

func TestRecordSignal(t *testing.T) {
	for _, tt := range []struct {
		input adminapi.RecordSignal
		want  signal.Signal
	}{
		{adminapi.RecordSignalLogs, signal.Log},
		{adminapi.RecordSignalTraces, signal.Trace},
		{adminapi.RecordSignalProfiles, signal.Profile},
		{adminapi.RecordSignalExemplars, signal.Exemplar},
	} {
		t.Run(string(tt.input), func(t *testing.T) {
			got, err := recordSignal(tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
