package logsbench

import (
	"embed"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-faster/errors"
)

// Assets contains the embedded logs benchmark compose files, service configs, and query suites.
//
//go:embed embed/*.yml embed/*.xml embed/suites/*.yml
var Assets embed.FS

// WriteAssets materializes embedded benchmark assets into dir.
func WriteAssets(dir string) error {
	return fs.WalkDir(Assets, "embed", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel := strings.TrimPrefix(path, "embed")
		if rel == "" {
			return nil
		}
		target := filepath.Join(dir, filepath.FromSlash(rel))
		if entry.IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return errors.Wrap(err, "create asset directory")
			}
			return nil
		}
		data, err := Assets.ReadFile(path)
		if err != nil {
			return errors.Wrap(err, "read asset")
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return errors.Wrap(err, "create asset parent")
		}
		if err := os.WriteFile(target, data, 0o644); err != nil {
			return errors.Wrap(err, "write asset")
		}
		return nil
	})
}

// DefaultSuitePath returns the path to the embedded Loghub LogQL suite after WriteAssets.
func DefaultSuitePath(workDir string) string {
	return filepath.Join(workDir, "suites", "logql-loghub.yml")
}
