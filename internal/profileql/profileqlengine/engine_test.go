package profileqlengine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/profileql"
	"github.com/oteldb/oteldb/internal/profilestorage"
)

// mockQuerier is a configurable [profilestorage.Querier] for tests.
type mockQuerier struct {
	profileTypes func(ctx context.Context, opts profilestorage.ProfileTypesOptions) ([]profileql.ProfileType, error)
	labelNames   func(ctx context.Context, opts profilestorage.LabelNamesOptions) ([]string, error)
	labelValues  func(ctx context.Context, label string, opts profilestorage.LabelValuesOptions) ([]string, error)
	selectMerge  func(ctx context.Context, params profilestorage.SelectProfileParams) (*profilestorage.FlameTree, error)
}

func (m mockQuerier) ProfileTypes(ctx context.Context, opts profilestorage.ProfileTypesOptions) ([]profileql.ProfileType, error) {
	return m.profileTypes(ctx, opts)
}

func (m mockQuerier) LabelNames(ctx context.Context, opts profilestorage.LabelNamesOptions) ([]string, error) {
	return m.labelNames(ctx, opts)
}

func (m mockQuerier) LabelValues(ctx context.Context, label string, opts profilestorage.LabelValuesOptions) ([]string, error) {
	return m.labelValues(ctx, label, opts)
}

func (m mockQuerier) SelectMergeProfile(ctx context.Context, params profilestorage.SelectProfileParams) (*profilestorage.FlameTree, error) {
	return m.selectMerge(ctx, params)
}

var _ profilestorage.Querier = mockQuerier{}

func mustType(t *testing.T, s string) profileql.ProfileType {
	t.Helper()
	pt, err := profileql.ParseProfileType(s)
	require.NoError(t, err)
	return pt
}

func TestEngineEval(t *testing.T) {
	var gotParams profilestorage.SelectProfileParams
	q := mockQuerier{
		selectMerge: func(_ context.Context, params profilestorage.SelectProfileParams) (*profilestorage.FlameTree, error) {
			gotParams = params
			return &profilestorage.FlameTree{
				Root: &profilestorage.FlameNode{
					Name: "root", Total: 5, Self: 0,
					Children: []*profilestorage.FlameNode{
						{Name: "main", Total: 5, Self: 5},
					},
				},
			}, nil
		},
	}
	e := NewEngine(q, Options{})

	profile, err := e.Eval(context.Background(),
		`process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name="frontend"}`,
		EvalParams{},
	)
	require.NoError(t, err)

	// Query was parsed and forwarded to the querier.
	require.Equal(t, "process_cpu", gotParams.Type.Name)
	require.Len(t, gotParams.Matchers, 1)
	require.Equal(t, profileql.Label("service_name"), gotParams.Matchers[0].Label)

	// Flamebearer was rendered.
	require.Equal(t, []string{"total", "main"}, profile.Flamebearer.Names)
	require.Equal(t, 5, profile.Flamebearer.NumTicks)
	require.Equal(t, "single", profile.Metadata.Format)
	require.Equal(t, "cpu", profile.Metadata.Name.Value)
	require.Equal(t, uint32(1_000_000_000), profile.Metadata.SampleRate.Value)
}

func TestEngineEvalParseError(t *testing.T) {
	e := NewEngine(mockQuerier{}, Options{})
	_, err := e.Eval(context.Background(), "not a valid query", EvalParams{})
	require.Error(t, err)
}

func TestEngineEvalEmptyResult(t *testing.T) {
	q := mockQuerier{
		selectMerge: func(context.Context, profilestorage.SelectProfileParams) (*profilestorage.FlameTree, error) {
			return &profilestorage.FlameTree{}, nil
		},
	}
	e := NewEngine(q, Options{})
	profile, err := e.Eval(context.Background(),
		`process_cpu:cpu:nanoseconds:cpu:nanoseconds`,
		EvalParams{},
	)
	require.NoError(t, err)
	require.Empty(t, profile.Flamebearer.Names)
	require.Equal(t, 0, profile.Flamebearer.NumTicks)
}
