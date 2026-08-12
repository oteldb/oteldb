package ch2storagebackend

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/go-faster/errors"
)

// Checkpoint records which (signal, UTC day) slices of a migration have been fully ingested *and*
// made durable in the target engine, so an interrupted run resumes instead of restarting.
//
// It is a line-delimited JSON journal, appended and fsynced one line per completed day. Append-only
// is what makes it crash-safe without a write-ahead protocol of its own: a torn trailing line is
// simply a day that was not confirmed, and re-migrating a day is harmless — the target engine is
// keyed by series identity and timestamp, so a replayed day overwrites rather than duplicates.
type Checkpoint struct {
	mu   sync.Mutex
	f    *os.File
	done map[checkpointKey]struct{}
}

type checkpointKey struct {
	signal string
	day    int64
}

// checkpointEntry is one journal line.
type checkpointEntry struct {
	Signal string    `json:"signal"`
	Day    time.Time `json:"day"`
	Rows   int       `json:"rows"`
	At     time.Time `json:"at"`
}

// OpenCheckpoint opens (creating if absent) the journal at path and replays it into memory.
func OpenCheckpoint(path string) (*Checkpoint, error) {
	done, err := readCheckpoint(path)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644) //nolint:gosec // operator-supplied journal path
	if err != nil {
		return nil, errors.Wrap(err, "open checkpoint")
	}
	return &Checkpoint{f: f, done: done}, nil
}

// readCheckpoint replays the journal. A malformed trailing line is treated as an interrupted
// append and ignored; a malformed line anywhere else is a real corruption and is reported.
func readCheckpoint(path string) (map[checkpointKey]struct{}, error) {
	done := map[checkpointKey]struct{}{}

	f, err := os.Open(path) //nolint:gosec // operator-supplied journal path
	if err != nil {
		if os.IsNotExist(err) {
			return done, nil
		}
		return nil, errors.Wrap(err, "open checkpoint")
	}
	defer func() {
		_ = f.Close()
	}()

	var lines [][]byte
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		if line := sc.Bytes(); len(line) > 0 {
			lines = append(lines, append([]byte(nil), line...))
		}
	}
	if err := sc.Err(); err != nil {
		return nil, errors.Wrap(err, "read checkpoint")
	}

	for i, line := range lines {
		var e checkpointEntry
		if err := json.Unmarshal(line, &e); err != nil {
			// Only the final line can be a torn append. Anywhere else it is real corruption, and
			// silently skipping it would resume the migration from a wrong position.
			if i == len(lines)-1 {
				break
			}
			return nil, errors.Wrapf(err, "parse checkpoint %s line %d", path, i+1)
		}
		done[checkpointKey{signal: e.Signal, day: e.Day.UTC().Unix()}] = struct{}{}
	}
	return done, nil
}

// Done reports whether the (signal, day) slice has already been migrated.
func (c *Checkpoint) Done(signal string, day time.Time) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.done[checkpointKey{signal: signal, day: day.UTC().Unix()}]
	return ok
}

// Mark durably records the (signal, day) slice as migrated. The caller must have already made the
// slice's data durable in the target engine — marking first would turn a crash into silent data
// loss on resume.
func (c *Checkpoint) Mark(signal string, day time.Time, rows int) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	line, err := json.Marshal(checkpointEntry{
		Signal: signal,
		Day:    day.UTC(),
		Rows:   rows,
		At:     time.Now().UTC(),
	})
	if err != nil {
		return errors.Wrap(err, "encode checkpoint entry")
	}
	if _, err := c.f.Write(append(line, '\n')); err != nil {
		return errors.Wrap(err, "append checkpoint")
	}
	if err := c.f.Sync(); err != nil {
		return errors.Wrap(err, "sync checkpoint")
	}

	c.done[checkpointKey{signal: signal, day: day.UTC().Unix()}] = struct{}{}
	return nil
}

// Close releases the journal file.
func (c *Checkpoint) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.f.Close(); err != nil {
		return errors.Wrap(err, "close checkpoint")
	}
	return nil
}
