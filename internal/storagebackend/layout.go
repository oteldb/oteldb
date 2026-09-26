package storagebackend

import (
	"cmp"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-faster/errors"
)

const (
	partsSubdir = "parts"
	walSubdir   = "wal"
)

// Layout is where the engine keeps its state on local disk.
type Layout struct {
	// Parts is the file backend root. Empty when the backend keeps no parts locally.
	Parts string
	// WAL is the write-ahead log directory. Empty when the engine runs without one.
	WAL string
}

// Layout resolves the local directories of the configured backend. For the file backend it refuses
// a WAL that overlaps the parts directory, and a data directory still in the layout that predates
// the parts subdirectory.
func (cfg *Config) Layout() (Layout, error) {
	switch cfg.Backend {
	case "", "memory":
		return Layout{}, nil
	case "file":
		if cfg.Dir == "" {
			return Layout{}, errors.New("storage.dir is required for the file backend")
		}
		l := Layout{
			Parts: filepath.Join(cfg.Dir, partsSubdir),
			WAL:   cmp.Or(cfg.WALDir, filepath.Join(cfg.Dir, walSubdir)),
		}
		if err := checkDisjoint(l.Parts, l.WAL); err != nil {
			return Layout{}, err
		}
		if err := checkPartsLayout(cfg.Dir, l.WAL); err != nil {
			return Layout{}, err
		}
		return l, nil
	case "s3":
		return Layout{WAL: cfg.WALDir}, nil
	default:
		return Layout{}, errors.Errorf("unknown storage backend %q", cfg.Backend)
	}
}

// checkDisjoint refuses a WAL directory equal to, inside, or containing the parts directory: the
// file backend owns every entry under its root, and the WAL owns every entry under its own.
func checkDisjoint(parts, wal string) error {
	p, err := resolvePath(parts)
	if err != nil {
		return errors.Wrap(err, "resolve parts directory")
	}
	w, err := resolvePath(wal)
	if err != nil {
		return errors.Wrap(err, "resolve storage.wal_dir")
	}
	if within(p, w) || within(w, p) {
		return errors.Errorf("storage.wal_dir %q overlaps the parts directory %q", wal, parts)
	}
	return nil
}

func within(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// resolvePath returns the absolute, symlink-resolved form of path. A path that does not exist yet
// resolves through its deepest existing ancestor.
func resolvePath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	dir, rest := abs, ""
	for {
		resolved, err := filepath.EvalSymlinks(dir)
		switch {
		case err == nil:
			return filepath.Join(resolved, rest), nil
		case !errors.Is(err, fs.ErrNotExist):
			return "", err
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return abs, nil
		}
		rest = filepath.Join(filepath.Base(dir), rest)
		dir = parent
	}
}

// checkPartsLayout fails on a data directory that holds tenant data directly under dir rather than
// under dir/parts. Opening it would start an empty store next to the data.
func checkPartsLayout(dir, wal string) error {
	entries, err := os.ReadDir(dir)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil
	case err != nil:
		return errors.Wrap(err, "read storage.dir")
	}
	walInfo, walErr := os.Stat(wal)
	for _, e := range entries {
		if e.Name() == partsSubdir {
			return nil
		}
	}
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() || name == walSubdir || name == "lost+found" || strings.HasPrefix(name, ".") {
			continue
		}
		if walErr == nil {
			if info, err := os.Stat(filepath.Join(dir, name)); err == nil && os.SameFile(info, walInfo) {
				continue
			}
		}
		return errors.Errorf("storage.dir %q holds data outside %q (found %q): stop the node, "+
			"create %q and move every top-level entry of %q except %q into it",
			dir, partsSubdir, name, filepath.Join(dir, partsSubdir), dir, walSubdir)
	}
	return nil
}
