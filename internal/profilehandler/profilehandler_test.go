package profilehandler

import (
	"context"
	"net/http"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/profileql"
	"github.com/oteldb/oteldb/internal/profileql/profileqlengine"
	"github.com/oteldb/oteldb/internal/profilestorage"
	"github.com/oteldb/oteldb/internal/pyroscopeapi"
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

func newAPI(q profilestorage.Querier) *PyroscopeAPI {
	engine := profileqlengine.NewEngine(q, profileqlengine.Options{})
	return NewPyroscopeAPI(q, engine, PyroscopeAPIOptions{})
}

func statusCode(t *testing.T, err error) int {
	t.Helper()
	var sc *pyroscopeapi.ErrorStatusCode
	require.ErrorAs(t, err, &sc)
	return sc.StatusCode
}

func TestGetApps(t *testing.T) {
	cpu, err := profileql.ParseProfileType("process_cpu:cpu:nanoseconds:cpu:nanoseconds")
	require.NoError(t, err)
	h := newAPI(mockQuerier{
		profileTypes: func(context.Context, profilestorage.ProfileTypesOptions) ([]profileql.ProfileType, error) {
			return []profileql.ProfileType{cpu}, nil
		},
	})

	apps, err := h.GetApps(context.Background())
	require.NoError(t, err)
	require.Len(t, apps, 1)
	require.Equal(t, "process_cpu", apps[0].Name.Value)
	require.Equal(t, pyroscopeapi.ApplicationMetadataUnitsSamples, apps[0].Units.Value)
}

func TestIngestNotImplemented(t *testing.T) {
	h := newAPI(mockQuerier{})
	err := h.Ingest(context.Background(), &pyroscopeapi.IngestReqWithContentType{}, pyroscopeapi.IngestParams{})
	require.Error(t, err)
}

func TestLabels(t *testing.T) {
	var gotOpts profilestorage.LabelNamesOptions
	h := newAPI(mockQuerier{
		labelNames: func(_ context.Context, opts profilestorage.LabelNamesOptions) ([]string, error) {
			gotOpts = opts
			return []string{"service_name", "env"}, nil
		},
	})

	labels, err := h.Labels(context.Background(), pyroscopeapi.LabelsParams{
		Query: pyroscopeapi.NewOptString(`process_cpu:cpu:nanoseconds:cpu:nanoseconds{env="prod"}`),
	})
	require.NoError(t, err)
	require.Equal(t, pyroscopeapi.Labels{"service_name", "env"}, labels)

	// Query was parsed into a type and matchers.
	require.NotNil(t, gotOpts.Type)
	require.Equal(t, "process_cpu", gotOpts.Type.Name)
	require.Len(t, gotOpts.Matchers, 1)
}

func TestLabelsInvalidQuery(t *testing.T) {
	h := newAPI(mockQuerier{})
	_, err := h.Labels(context.Background(), pyroscopeapi.LabelsParams{
		Query: pyroscopeapi.NewOptString("definitely not valid"),
	})
	require.Equal(t, http.StatusBadRequest, statusCode(t, err))
}

func TestLabelValues(t *testing.T) {
	h := newAPI(mockQuerier{
		labelValues: func(_ context.Context, label string, _ profilestorage.LabelValuesOptions) ([]string, error) {
			require.Equal(t, "env", label)
			return []string{"prod", "staging"}, nil
		},
	})

	values, err := h.LabelValues(context.Background(), pyroscopeapi.LabelValuesParams{
		Label: "env",
	})
	require.NoError(t, err)
	require.Equal(t, pyroscopeapi.LabelValues{"prod", "staging"}, values)
}

func TestLabelValuesRequiresLabel(t *testing.T) {
	h := newAPI(mockQuerier{})
	_, err := h.LabelValues(context.Background(), pyroscopeapi.LabelValuesParams{})
	require.Equal(t, http.StatusBadRequest, statusCode(t, err))
}

func TestLabelValuesProfileType(t *testing.T) {
	cpu, err := profileql.ParseProfileType("process_cpu:cpu:nanoseconds:cpu:nanoseconds")
	require.NoError(t, err)
	h := newAPI(mockQuerier{
		profileTypes: func(context.Context, profilestorage.ProfileTypesOptions) ([]profileql.ProfileType, error) {
			return []profileql.ProfileType{cpu}, nil
		},
	})

	values, err := h.LabelValues(context.Background(), pyroscopeapi.LabelValuesParams{
		Label: profileql.LabelProfileType,
	})
	require.NoError(t, err)
	require.Equal(t, pyroscopeapi.LabelValues{cpu.ID()}, values)
}

func TestRender(t *testing.T) {
	h := newAPI(mockQuerier{
		selectMerge: func(context.Context, profilestorage.SelectProfileParams) (*profilestorage.FlameTree, error) {
			return &profilestorage.FlameTree{
				Root: &profilestorage.FlameNode{
					Name: "root", Total: 3, Self: 0,
					Children: []*profilestorage.FlameNode{
						{Name: "main", Total: 3, Self: 3},
					},
				},
			}, nil
		},
	})

	profile, err := h.Render(context.Background(), pyroscopeapi.RenderParams{
		Query:  pyroscopeapi.NewOptString(`process_cpu:cpu:nanoseconds:cpu:nanoseconds`),
		Format: pyroscopeapi.RenderFormatJSON,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"total", "main"}, profile.Flamebearer.Names)
	require.Equal(t, 3, profile.Flamebearer.NumTicks)
}

func TestRenderRequiresQuery(t *testing.T) {
	h := newAPI(mockQuerier{})
	_, err := h.Render(context.Background(), pyroscopeapi.RenderParams{Format: pyroscopeapi.RenderFormatJSON})
	require.Equal(t, http.StatusBadRequest, statusCode(t, err))
}

func TestRenderUnsupportedFormat(t *testing.T) {
	h := newAPI(mockQuerier{})
	_, err := h.Render(context.Background(), pyroscopeapi.RenderParams{
		Query:  pyroscopeapi.NewOptString(`process_cpu:cpu:nanoseconds:cpu:nanoseconds`),
		Format: pyroscopeapi.RenderFormatPprof,
	})
	require.Equal(t, http.StatusBadRequest, statusCode(t, err))
}

func TestNewError(t *testing.T) {
	h := newAPI(mockQuerier{})

	// Wrapped ErrorStatusCode is passed through.
	orig := &pyroscopeapi.ErrorStatusCode{StatusCode: http.StatusTeapot, Response: "boom"}
	got := h.NewError(context.Background(), errors.Wrap(orig, "context"))
	require.Equal(t, http.StatusTeapot, got.StatusCode)

	// Plain error becomes a 500.
	got = h.NewError(context.Background(), errors.New("plain"))
	require.Equal(t, http.StatusInternalServerError, got.StatusCode)
}
