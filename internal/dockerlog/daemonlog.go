package dockerlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

// ParseLog parses log stream from Docker daemon.
func ParseLog(f io.ReadCloser, resource otelstorage.Attrs) logqlengine.EntryIterator {
	return &streamIter{
		rd:       f,
		err:      nil,
		resource: resource,
	}
}

const headerLen = 8

type streamIter struct {
	rd     io.ReadCloser
	header [headerLen]byte
	buf    bytes.Buffer
	err    error

	resource otelstorage.Attrs
}

var _ logqlengine.EntryIterator = (*streamIter)(nil)

// Next returns true, if there is element and fills t.
func (i *streamIter) Next(r *logqlengine.Entry) (ok bool) {
	// Reset entry.
	*r = logqlengine.Entry{
		Set: logqlabels.NewLabelSet(),
	}

	ok, i.err = i.parseNext(r)
	return ok
}

type stdType byte

const (
	// Stdin represents standard input stream type.
	stdin stdType = iota
	// Stdout represents standard output stream type.
	stdout
	// Stderr represents standard error steam type.
	stderr
	// Systemerr represents errors originating from the system that make it
	// into the multiplexed stream.
	systemerr
)

func (t stdType) String() string {
	switch t {
	case stdin:
		return "stdin"
	case stdout:
		return "stdout"
	case stderr:
		return "stderr"
	case systemerr:
		return "<system error>"
	default:
		return fmt.Sprintf("unknown type %d", byte(t))
	}
}

func (i *streamIter) parseNext(r *logqlengine.Entry) (bool, error) {
	if _, err := io.ReadFull(i.rd, i.header[:]); err != nil {
		switch err {
		case io.EOF, io.ErrUnexpectedEOF:
			// Handle missing header gracefully, docker-cli does the same thing.
			return false, nil
		default:
			return false, errors.Wrap(err, "read header")
		}
	}

	var (
		typ       = stdType(i.header[0])
		frameSize = binary.BigEndian.Uint32(i.header[4:8])
	)
	i.buf.Reset()
	if _, err := io.CopyN(&i.buf, i.rd, int64(frameSize)); err != nil {
		return false, errors.Wrap(err, "read message")
	}
	if typ == systemerr {
		return false, errors.Errorf("daemon log stream error: %q", &i.buf)
	}

	if err := parseDockerLine(typ, i.buf.String(), r); err != nil {
		return false, errors.Wrap(err, "parse log line")
	}
	r.Set.SetAttrs(i.resource)

	return true, nil
}

func parseDockerLine(typ stdType, input string, r *logqlengine.Entry) error {
	const dockerTimestampFormat = time.RFC3339Nano

	rawTimestamp, line, ok := strings.Cut(input, " ")
	if !ok {
		return errors.New("invalid line: no space between timestamp and message")
	}
	r.Line = line

	ts, err := time.Parse(dockerTimestampFormat, rawTimestamp)
	if err != nil {
		return errors.Wrap(err, "parse timestamp")
	}
	r.Timestamp = otelstorage.NewTimestampFromTime(ts)

	r.Set.Set("stream", pcommon.NewValueStr(typ.String()))
	return nil
}

// Err returns an error caused during iteration, if any.
func (i *streamIter) Err() error {
	return i.err
}

// Close closes iterator.
func (i *streamIter) Close() error {
	return i.rd.Close()
}
