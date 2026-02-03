package chdump

import (
	"archive/tar"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"

	"github.com/go-faster/errors"
)

// ReadTar reads dump from given tar file.
func ReadTar(r io.Reader) (TableReader, error) {
	tr := tar.NewReader(r)
	iter := &tarReader{tr: tr}
	return iter, nil
}

// ReadDir reads dump from unpacked directory.
func ReadDir(dir string) (TableReader, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrap(err, "read dir")
	}
	iter := &dirReader{dir: dir, entries: entries}
	return iter, nil
}

// TableReader is an interface to read tables from dump.
type TableReader interface {
	Next() (string, io.ReadCloser, error)
}

type tarReader struct {
	tr *tar.Reader
}

func (t *tarReader) Next() (string, io.ReadCloser, error) {
	h, err := t.tr.Next()
	if err != nil {
		return "", nil, err
	}
	name := path.Clean(h.Name)
	return name, io.NopCloser(t.tr), nil
}

type dirReader struct {
	dir     string
	entries []fs.DirEntry
	idx     int
}

func (d *dirReader) Next() (string, io.ReadCloser, error) {
	for {
		if d.idx >= len(d.entries) {
			return "", nil, io.EOF
		}

		entry := d.entries[d.idx]
		if entry.IsDir() {
			d.idx++
			continue
		}

		f, err := os.Open(filepath.Join(d.dir, entry.Name()))
		if err != nil {
			return "", nil, err
		}
		name := filepath.Clean(entry.Name())

		d.idx++
		return name, f, nil
	}
}
