package metricscache

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash/fnv"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/go-faster/errors"
)

// diskMeta is the JSON metadata file for a single cache entry (<base>.json).
//
// The companion <base>.bin file holds the raw LZ4-compressed block data.
type diskMeta struct {
	Hash  string `json:"hash"` // hex-encoded [16]byte
	Step  int64  `json:"step"`
	Fn    string `json:"fn"`
	MinTS int64  `json:"min_ts"`
	MaxTS int64  `json:"max_ts"`
	Count int    `json:"count"`
}

// DiskStore is a directory-based persistent Store.
//
// Each entry is stored as a pair of files:
//   - <base>.json — human-readable metadata (key fields, watermarks, point count)
//   - <base>.bin  — raw LZ4-compressed block payload (DoubleDelta timestamps + float64 values)
//
// An in-memory index maps Key → base filename for fast lookup.
// There is no eviction in v1: entries persist until the directory is cleared.
type DiskStore struct {
	dir      string
	mu       sync.RWMutex
	idx      map[Key]string // key → base filename (without extension)
	rejected atomic.Int64
}

// NewDiskStore creates a DiskStore rooted at dir.
func NewDiskStore(dir string) (*DiskStore, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, errors.Wrap(err, "create disk store dir")
	}
	s := &DiskStore{
		dir: dir,
		idx: make(map[Key]string),
	}
	if err := s.loadIndex(); err != nil {
		return nil, errors.Wrap(err, "load disk store index")
	}
	return s, nil
}

// loadIndex scans the directory and rebuilds the in-memory index from .json files.
// Corrupt or incomplete entries are deleted so they will be re-fetched.
func (s *DiskStore) loadIndex() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, de := range entries {
		if de.IsDir() || filepath.Ext(de.Name()) != ".json" {
			continue
		}
		base := strings.TrimSuffix(de.Name(), ".json")
		basePath := filepath.Join(s.dir, base)

		meta, key, err := s.readMetaFull(basePath + ".json")
		if err != nil {
			s.deleteEntry(basePath)
			continue
		}
		if meta.Count > 0 {
			if _, err := os.Stat(basePath + ".bin"); err != nil {
				s.deleteEntry(basePath)
				continue
			}
		}
		s.idx[key] = base
	}
	return nil
}

// Get retrieves an entry from disk.
// If the entry is corrupt or incomplete, it is deleted so it will be re-fetched.
func (s *DiskStore) Get(key Key) (*Entry, bool) {
	s.mu.RLock()
	base, ok := s.idx[key]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}

	basePath := filepath.Join(s.dir, base)
	entry, err := s.readEntry(basePath)
	if err != nil {
		s.deleteEntry(basePath)
		s.mu.Lock()
		delete(s.idx, key)
		s.mu.Unlock()
		return nil, false
	}
	return entry, true
}

// Set writes an entry to disk. Errors (e.g. ToBlock failure or disk I/O) are
// counted toward RejectedSets but otherwise ignored to match Store interface.
func (s *DiskStore) Set(key Key, entry *Entry) {
	block, err := entry.ToBlock()
	if err != nil {
		s.rejected.Add(1)
		return
	}

	base := keyBase(key)
	if err := s.writeEntry(filepath.Join(s.dir, base), key, block); err != nil {
		s.rejected.Add(1)
		return
	}

	s.mu.Lock()
	s.idx[key] = base
	s.mu.Unlock()
}

// Size returns the number of entries indexed in memory.
func (s *DiskStore) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.idx)
}

// diskSize walks the store directory and sums sizes of all files.
// It is approximate and intended for Stats() monitoring only.
func (s *DiskStore) diskSize() int64 {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return 0
	}
	var total int64
	for _, de := range entries {
		if de.IsDir() {
			continue
		}
		if fi, err := de.Info(); err == nil {
			total += fi.Size()
		}
	}
	return total
}

// Stats returns current stats. DiskStore tracks Size, RejectedSets and DiskSize.
func (s *DiskStore) Stats() StoreStats {
	return StoreStats{
		Size:         s.Size(),
		RejectedSets: s.rejected.Load(),
		DiskSize:     s.diskSize(),
	}
}

// Close is a no-op for DiskStore.
func (s *DiskStore) Close() error {
	return nil
}

// writeEntry atomically writes <base>.bin then <base>.json for the given entry.
//
// The .bin file is written first; if the process crashes between the two renames,
// the orphaned .bin is harmless (loadIndex only looks at .json files).
func (s *DiskStore) writeEntry(base string, key Key, block Block) error {
	// Write block data.
	if err := atomicWrite(s.dir, base+".bin", block.Data); err != nil {
		return errors.Wrap(err, "write .bin")
	}

	// Write JSON metadata.
	meta := diskMeta{
		Hash:  hex.EncodeToString(key.Hash[:]),
		Step:  key.Step,
		Fn:    key.Fn,
		MinTS: block.MinTS,
		MaxTS: block.MaxTS,
		Count: block.Count,
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return errors.Wrap(err, "marshal meta")
	}
	if err := atomicWrite(s.dir, base+".json", data); err != nil {
		return errors.Wrap(err, "write .json")
	}
	return nil
}

// atomicWrite writes data to path via a temp file + rename.
func atomicWrite(dir, path string, data []byte) error {
	f, err := os.CreateTemp(dir, filepath.Base(path)+"-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	closed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
		_ = os.Remove(tmp)
	}()

	if _, err := f.Write(data); err != nil {
		return err
	}
	closed = true
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// readMetaFull reads the .json metadata file and returns both the parsed struct and the Key.
func (s *DiskStore) readMetaFull(path string) (diskMeta, Key, error) {
	data, err := os.ReadFile(path) // #nosec G304
	if err != nil {
		return diskMeta{}, Key{}, err
	}
	var meta diskMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return diskMeta{}, Key{}, errors.Wrap(err, "unmarshal meta")
	}

	hashBytes, err := hex.DecodeString(meta.Hash)
	if err != nil {
		return diskMeta{}, Key{}, errors.Wrap(err, "decode hash")
	}
	if len(hashBytes) != 16 {
		return diskMeta{}, Key{}, errors.New("invalid hash length")
	}

	var key Key
	copy(key.Hash[:], hashBytes)
	key.Step = meta.Step
	key.Fn = meta.Fn
	return meta, key, nil
}

// deleteEntry removes the .json and .bin files for the given base path.
//
// It is intentionally not locked: concurrent calls (e.g. two Get calls
// concurrently discovering a corrupt entry for the same key) may both
// invoke os.Remove on the same paths. This is harmless — the second
// Remove returns "file not found" which is ignored.
func (s *DiskStore) deleteEntry(base string) {
	_ = os.Remove(base + ".json")
	_ = os.Remove(base + ".bin")
}

// readEntry reads the .json + .bin pair and returns a decoded Entry.
func (s *DiskStore) readEntry(base string) (*Entry, error) {
	meta, _, err := s.readMetaFull(base + ".json")
	if err != nil {
		return nil, err
	}

	var blockData []byte
	if meta.Count > 0 {
		blockData, err = os.ReadFile(base + ".bin") // #nosec G304
		if err != nil {
			return nil, errors.Wrap(err, "read .bin")
		}
		if len(blockData) > 16*1024*1024 {
			return nil, errors.New("block data too large")
		}
	}

	block := Block{
		Data:  blockData,
		MinTS: meta.MinTS,
		MaxTS: meta.MaxTS,
		Count: meta.Count,
	}
	return FromBlock(block)
}

// keyBase returns a stable base filename (without extension) for the given key.
func keyBase(key Key) string {
	h := fnv.New128a()
	h.Write(key.Hash[:])
	var stepBuf [8]byte
	binary.LittleEndian.PutUint64(stepBuf[:], uint64(key.Step))
	h.Write(stepBuf[:])
	h.Write([]byte(key.Fn))
	return hex.EncodeToString(h.Sum(nil))
}
