package logsbench

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-faster/yaml"

	"github.com/oteldb/oteldb/cmd/otelbench/logqlbench"
	"github.com/oteldb/oteldb/internal/logql"
)

func TestWriteAssets(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if err := WriteAssets(dir); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"docker-compose.yml",
		"clickhouse.xml",
		"oteldb.yml",
		"otelcol.yml",
		"tempo.yml",
		filepath.Join("suites", "logql-loghub.yml"),
	} {
		if _, err := os.Stat(filepath.Join(dir, path)); err != nil {
			t.Fatalf("asset %q: %v", path, err)
		}
	}
}

func TestLoghubSuiteQueriesParse(t *testing.T) {
	t.Parallel()

	data, err := Assets.ReadFile("embed/suites/logql-loghub.yml")
	if err != nil {
		t.Fatal(err)
	}
	var input logqlbench.Input
	if err := yaml.Unmarshal(data, &input); err != nil {
		t.Fatal(err)
	}

	opts := logql.ParseOptions{AllowDots: true}
	for _, q := range input.Instant {
		if _, err := logql.Parse(q.LogQL, opts); err != nil {
			t.Fatalf("instant query %q: %v", q.LogQL, err)
		}
	}
	for _, q := range input.Range {
		if _, err := logql.Parse(q.LogQL, opts); err != nil {
			t.Fatalf("range query %q: %v", q.LogQL, err)
		}
	}
	for _, q := range input.Series {
		for _, match := range q.Match {
			if _, err := logql.ParseSelector(match, opts); err != nil {
				t.Fatalf("series matcher %q: %v", match, err)
			}
		}
	}
	for _, q := range input.LabelValues {
		if q.Match == "" {
			continue
		}
		if _, err := logql.ParseSelector(q.Match, opts); err != nil {
			t.Fatalf("label values matcher %q: %v", q.Match, err)
		}
	}
}
