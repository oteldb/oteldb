package logsbench

import (
	"os"
	"path/filepath"
	"testing"
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
