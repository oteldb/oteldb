package logsbench

import (
	"archive/tar"
	"compress/gzip"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestDownloadLoghubCachesAndExtracts(t *testing.T) {
	archive := makeTarGz(t, map[string]string{
		"Apache.log":     "line\n",
		"nested/SSH.log": "ssh\n",
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(archive)
	}))
	t.Cleanup(server.Close)

	oldBase := loghubBaseURLForTest
	loghubBaseURLForTest = server.URL
	t.Cleanup(func() { loghubBaseURLForTest = oldBase })

	result, err := DownloadLoghub(t.Context(), DownloadOptions{
		Size:     DatasetSmall,
		CacheDir: t.TempDir(),
		Client:   server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(result.Dir, "Apache.log")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(result.Dir, "nested", "SSH.log")); err != nil {
		t.Fatal(err)
	}
}

func TestExtractTarGzRejectsUnsafePath(t *testing.T) {
	t.Parallel()

	archive := filepath.Join(t.TempDir(), "unsafe.tar.gz")
	writeTarGz(t, archive, map[string]string{"../escape.log": "bad"})
	if err := extractTarGz(archive, t.TempDir()); err == nil {
		t.Fatal("extractTarGz succeeded for unsafe path")
	}
}

func makeTarGz(t *testing.T, files map[string]string) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "data.tar.gz")
	writeTarGz(t, path, files)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func writeTarGz(t *testing.T, path string, files map[string]string) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(file)
	tw := tar.NewWriter(gz)
	for name, body := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}
