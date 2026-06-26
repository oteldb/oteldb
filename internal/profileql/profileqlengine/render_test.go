package profileqlengine

import (
	"strings"
	"testing"

	"github.com/google/pprof/profile"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/profilestorage"
)

func testResult(t *testing.T) *Result {
	return &Result{
		Tree: &profilestorage.FlameTree{
			Root: &profilestorage.FlameNode{
				Name: "root", Total: 10, Self: 0,
				Children: []*profilestorage.FlameNode{
					{
						Name: "a", Total: 6, Self: 2,
						Children: []*profilestorage.FlameNode{
							{Name: "a1", Total: 4, Self: 4},
						},
					},
					{Name: "b", Total: 4, Self: 4},
				},
			},
		},
		Type: mustType(t, "process_cpu:cpu:nanoseconds:cpu:nanoseconds"),
	}
}

func TestResultCollapsed(t *testing.T) {
	got := string(testResult(t).Collapsed())
	lines := strings.Split(strings.TrimSpace(got), "\n")
	require.ElementsMatch(t, []string{
		"a 2",
		"a;a1 4",
		"b 4",
	}, lines)
}

func TestResultCollapsedEmpty(t *testing.T) {
	require.Nil(t, (&Result{}).Collapsed())
	require.Nil(t, (&Result{Tree: &profilestorage.FlameTree{}}).Collapsed())
}

func TestResultPprof(t *testing.T) {
	data, err := testResult(t).Pprof()
	require.NoError(t, err)

	p, err := profile.ParseData(data)
	require.NoError(t, err)
	require.NoError(t, p.CheckValid())

	require.Len(t, p.SampleType, 1)
	require.Equal(t, "cpu", p.SampleType[0].Type)
	require.Equal(t, "nanoseconds", p.SampleType[0].Unit)

	// Three nodes have non-zero self values: a, a1, b.
	require.Len(t, p.Sample, 3)

	// Collect stacks (leaf-first function names) with their values.
	stacks := map[string]int64{}
	for _, s := range p.Sample {
		var names []string
		for _, loc := range s.Location {
			names = append(names, loc.Line[0].Function.Name)
		}
		stacks[strings.Join(names, ";")] = s.Value[0]
	}
	require.Equal(t, int64(2), stacks["a"])
	require.Equal(t, int64(4), stacks["a1;a"])
	require.Equal(t, int64(4), stacks["b"])
}

func TestResultHTML(t *testing.T) {
	data, err := testResult(t).HTML()
	require.NoError(t, err)

	html := string(data)
	require.Contains(t, html, "<!DOCTYPE html>")
	require.Contains(t, html, "window.flamegraph =")
	// Embedded flamebearer data is present.
	require.Contains(t, html, `"names"`)
	require.Contains(t, html, `"total"`)
}
