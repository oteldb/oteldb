package logsbench

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/go-faster/errors"
)

// DatasetSize is a supported Loghub benchmark data tier.
type DatasetSize string

const (
	DatasetSmall  DatasetSize = "small"
	DatasetMedium DatasetSize = "medium"
	DatasetLarge  DatasetSize = "large"
)

const loghubBaseURL = "https://zenodo.org/records/3227177/files"

var loghubBaseURLForTest = loghubBaseURL

// DatasetFile is a file included in a Loghub data tier.
type DatasetFile struct {
	Name string
	URL  string
}

// DownloadOptions configures Loghub dataset download and extraction.
type DownloadOptions struct {
	Size     DatasetSize
	CacheDir string
	Client   *http.Client
}

// DownloadResult describes the cached dataset location.
type DownloadResult struct {
	Dir   string
	Files []DatasetFile
}

// DefaultCacheDir returns the default otelbench Loghub cache directory.
func DefaultCacheDir() string {
	if dir := os.Getenv("XDG_CACHE_HOME"); dir != "" {
		return filepath.Join(dir, "otelbench", "loghub")
	}
	if home, err := os.UserCacheDir(); err == nil && home != "" {
		return filepath.Join(home, "otelbench", "loghub")
	}
	return filepath.Join(os.TempDir(), "otelbench", "loghub")
}

// LoghubFiles returns the Loghub files needed for size.
func LoghubFiles(size DatasetSize) ([]DatasetFile, error) {
	files := []DatasetFile{
		loghubFile("Linux"),
		loghubFile("Apache"),
		loghubFile("SSH"),
	}
	switch size {
	case "", DatasetSmall, DatasetMedium:
		return files, nil
	case DatasetLarge:
		return append(files, loghubFile("HDFS_2"), loghubFile("Thunderbird")), nil
	default:
		return nil, errors.Errorf("unknown dataset size %q", size)
	}
}

func loghubFile(name string) DatasetFile {
	return DatasetFile{
		Name: name,
		URL:  loghubBaseURLForTest + "/" + name + ".tar.gz?download=1",
	}
}

// DownloadLoghub downloads and extracts the selected Loghub tier into the cache.
func DownloadLoghub(ctx context.Context, opts DownloadOptions) (DownloadResult, error) {
	if opts.Size == "" {
		opts.Size = DatasetMedium
	}
	if opts.CacheDir == "" {
		opts.CacheDir = DefaultCacheDir()
	}
	client := opts.Client
	if client == nil {
		client = &http.Client{Timeout: 0}
	}
	files, err := LoghubFiles(opts.Size)
	if err != nil {
		return DownloadResult{}, err
	}
	dataDir := filepath.Join(opts.CacheDir, "data", string(opts.Size))
	archiveDir := filepath.Join(opts.CacheDir, "archives")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return DownloadResult{}, errors.Wrap(err, "create data cache")
	}
	if err := os.MkdirAll(archiveDir, 0o755); err != nil {
		return DownloadResult{}, errors.Wrap(err, "create archive cache")
	}
	for _, file := range files {
		archivePath := filepath.Join(archiveDir, file.Name+".tar.gz")
		if err := downloadFile(ctx, client, file.URL, archivePath); err != nil {
			return DownloadResult{}, errors.Wrapf(err, "download %s", file.Name)
		}
		marker := filepath.Join(dataDir, "."+file.Name+".ready")
		if _, err := os.Stat(marker); err == nil {
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return DownloadResult{}, errors.Wrap(err, "stat marker")
		}
		if err := extractTarGz(archivePath, dataDir); err != nil {
			return DownloadResult{}, errors.Wrapf(err, "extract %s", file.Name)
		}
		if err := os.WriteFile(marker, []byte(time.Now().Format(time.RFC3339Nano)+"\n"), 0o644); err != nil {
			return DownloadResult{}, errors.Wrap(err, "write marker")
		}
	}
	return DownloadResult{Dir: dataDir, Files: files}, nil
}

func downloadFile(ctx context.Context, client *http.Client, url, path string) error {
	if info, err := os.Stat(path); err == nil && info.Size() > 0 {
		return nil
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.Wrap(err, "stat archive")
	}

	partPath := path + ".part"
	var offset int64
	if info, err := os.Stat(partPath); err == nil {
		offset = info.Size()
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.Wrap(err, "stat partial archive")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return errors.Wrap(err, "create request")
	}
	if offset > 0 {
		req.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")
	}
	resp, err := client.Do(req)
	if err != nil {
		return errors.Wrap(err, "send request")
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return errors.Errorf("unexpected status %s", resp.Status)
	}
	if offset > 0 && resp.StatusCode == http.StatusOK {
		offset = 0
	}
	flag := os.O_CREATE | os.O_WRONLY
	if offset > 0 {
		flag |= os.O_APPEND
	} else {
		flag |= os.O_TRUNC
	}
	out, err := os.OpenFile(partPath, flag, 0o644) // #nosec G302,G304 -- partial download file in controlled cache dir; will be renamed
	if err != nil {
		return errors.Wrap(err, "open partial archive")
	}
	if _, err := io.Copy(out, resp.Body); err != nil {
		_ = out.Close()
		return errors.Wrap(err, "write partial archive")
	}
	if err := out.Close(); err != nil {
		return errors.Wrap(err, "close partial archive")
	}
	if err := os.Rename(partPath, path); err != nil {
		return errors.Wrap(err, "commit archive")
	}
	return nil
}

func extractTarGz(path, dir string) error {
	archive, err := os.Open(path) // #nosec G304 -- path is internal cache path constructed from known dataset names
	if err != nil {
		return errors.Wrap(err, "open archive")
	}
	defer func() {
		_ = archive.Close()
	}()
	gz, err := gzip.NewReader(archive)
	if err != nil {
		return errors.Wrap(err, "open gzip")
	}
	defer func() {
		_ = gz.Close()
	}()
	tr := tar.NewReader(gz)
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return errors.Wrap(err, "read tar")
		}
		if err := extractTarEntry(tr, header, dir); err != nil {
			return err
		}
	}
}

func extractTarEntry(reader io.Reader, header *tar.Header, dir string) error {
	if !filepath.IsLocal(header.Name) {
		return errors.Errorf("unsafe archive path %q", header.Name)
	}
	target := filepath.Join(dir, header.Name) // #nosec G305 -- filepath.IsLocal above ensures no path traversal
	switch header.Typeflag {
	case tar.TypeDir:
		if err := os.MkdirAll(target, 0o755); err != nil {
			return errors.Wrap(err, "create directory")
		}
	case tar.TypeReg, tar.TypeRegA: //nolint:staticcheck // TypeRegA is deprecated but still produced by some archives
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return errors.Wrap(err, "create file parent")
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644) // #nosec G302,G304 -- extracted dataset file in controlled cache dir; standard readable permissions for benchmark data
		if err != nil {
			return errors.Wrap(err, "create file")
		}
		if _, err := io.Copy(out, reader); err != nil {
			_ = out.Close()
			return errors.Wrap(err, "write file")
		}
		if err := out.Close(); err != nil {
			return errors.Wrap(err, "close file")
		}
	default:
		return nil
	}
	return nil
}
