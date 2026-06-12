package main

import (
	"bufio"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

const fileLogSpacing = 100 * time.Microsecond

var fileLogHosts = []string{"node-0", "node-1", "node-2", "node-3", "node-4", "node-5", "node-6", "node-7"}

type fileLogsSource struct {
	files           []fileLogInput
	entriesPerBatch int
	totalPasses     int
	limit           int64

	pass      int
	fileIndex int
	file      *os.File
	scanner   *bufio.Scanner
	nextTime  time.Time
	readLines int64
}

type fileLogInput struct {
	Path    string
	Service string
}

func newFileLogsSource(dir string, entriesPerBatch, repeat int, limit int64) (*fileLogsSource, error) {
	if repeat < 0 {
		return nil, errors.New("repeat must be non-negative")
	}
	if entriesPerBatch <= 0 {
		return nil, errors.New("entries per batch must be positive")
	}
	files, err := collectLogFiles(dir)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, errors.New("no .log files found")
	}
	return &fileLogsSource{
		files:           files,
		entriesPerBatch: entriesPerBatch,
		totalPasses:     repeat + 1,
		limit:           limit,
	}, nil
}

func collectLogFiles(dir string) ([]fileLogInput, error) {
	var files []fileLogInput
	if err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.EqualFold(filepath.Ext(path), ".log") {
			return nil
		}
		service := strings.ToLower(strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)))
		files = append(files, fileLogInput{
			Path:    path,
			Service: service,
		})
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "walk source")
	}
	slices.SortFunc(files, func(a, b fileLogInput) int {
		return strings.Compare(a.Path, b.Path)
	})
	return files, nil
}

func (s *fileLogsSource) Services() []string {
	services := make([]string, 0, len(s.files))
	seen := make(map[string]struct{}, len(s.files))
	for _, file := range s.files {
		if _, ok := seen[file.Service]; ok {
			continue
		}
		seen[file.Service] = struct{}{}
		services = append(services, file.Service)
	}
	return services
}

func (s *fileLogsSource) Close() error {
	return s.closeCurrent()
}

func (s *fileLogsSource) Next(_ *rand.Rand, ts time.Time) (plog.Logs, int64, int64, bool, error) {
	logs := plog.NewLogs()
	if s.nextTime.IsZero() {
		s.nextTime = ts
	}

	var lines, bytes int64
	for lines < int64(s.entriesPerBatch) {
		if s.limit > 0 && s.readLines >= s.limit {
			return logs, lines, bytes, true, nil
		}
		file, exhausted, err := s.currentFile()
		if err != nil || exhausted {
			return logs, lines, bytes, exhausted, err
		}
		if !s.scanner.Scan() {
			if err := s.scanner.Err(); err != nil {
				return logs, lines, bytes, false, errors.Wrap(err, "scan source")
			}
			if err := s.advanceFile(); err != nil {
				return logs, lines, bytes, false, err
			}
			continue
		}

		line := s.scanner.Text()
		s.nextTime = s.nextTime.Add(fileLogSpacing)
		appendFileLog(logs, file.Service, line, s.nextTime, s.readLines)
		lines++
		bytes += int64(len(line))
		s.readLines++
	}
	return logs, lines, bytes, false, nil
}

func (s *fileLogsSource) currentFile() (fileLogInput, bool, error) {
	if s.pass >= s.totalPasses {
		return fileLogInput{}, true, nil
	}
	if s.scanner != nil {
		return s.files[s.fileIndex], false, nil
	}
	file := s.files[s.fileIndex]
	reader, err := os.Open(file.Path)
	if err != nil {
		return fileLogInput{}, false, errors.Wrapf(err, "open %q", file.Path)
	}
	s.file = reader
	s.scanner = bufio.NewScanner(reader)
	s.scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	return file, false, nil
}

func (s *fileLogsSource) advanceFile() error {
	if err := s.closeCurrent(); err != nil {
		return err
	}
	s.fileIndex++
	if s.fileIndex < len(s.files) {
		return nil
	}
	s.fileIndex = 0
	s.pass++
	return nil
}

func (s *fileLogsSource) closeCurrent() error {
	s.scanner = nil
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	if err != nil && !errors.Is(err, os.ErrClosed) && !errors.Is(err, io.ErrClosedPipe) {
		return errors.Wrap(err, "close source")
	}
	return nil
}

func appendFileLog(logs plog.Logs, service, line string, ts time.Time, index int64) {
	resource := logs.ResourceLogs().AppendEmpty()
	attrs := resource.Resource().Attributes()
	attrs.PutStr("service.name", service)
	attrs.PutStr("host.name", fileLogHosts[int(index)%len(fileLogHosts)])
	scope := resource.ScopeLogs().AppendEmpty()
	record := scope.LogRecords().AppendEmpty()
	record.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	record.Body().SetStr(line)
	severity := detectLogSeverity(line)
	record.SetSeverityText(severity)
	record.SetSeverityNumber(logSeverityNumber(severity))
}

func detectLogSeverity(line string) string {
	upper := strings.ToUpper(line)
	for _, severity := range []string{"FATAL", "ERROR", "WARN", "INFO", "DEBUG"} {
		if strings.Contains(upper, severity) {
			return severity
		}
	}
	return "INFO"
}

func logSeverityNumber(severity string) plog.SeverityNumber {
	switch severity {
	case "FATAL":
		return plog.SeverityNumberFatal
	case "ERROR":
		return plog.SeverityNumberError
	case "WARN":
		return plog.SeverityNumberWarn
	case "DEBUG":
		return plog.SeverityNumberDebug
	default:
		return plog.SeverityNumberInfo
	}
}
