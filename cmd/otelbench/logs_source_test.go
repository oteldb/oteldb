package main

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileLogsSourceNext(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "SSH.log"), []byte("INFO accepted\nERROR failed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "Apache.log"), []byte("WARN slow\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	source, err := newFileLogsSource(dir, 2, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := source.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	logs, lines, bytes, exhausted, err := source.Next(rand.New(rand.NewSource(1)), time.Unix(10, 0))
	if err != nil {
		t.Fatal(err)
	}
	if exhausted {
		t.Fatal("source exhausted too early")
	}
	if lines != 2 {
		t.Fatalf("lines = %d, want 2", lines)
	}
	if bytes != int64(len("WARN slow")+len("INFO accepted")) {
		t.Fatalf("bytes = %d", bytes)
	}
	if logs.ResourceLogs().Len() != 2 {
		t.Fatalf("resource logs = %d, want 2", logs.ResourceLogs().Len())
	}

	first := logs.ResourceLogs().At(0)
	service, ok := first.Resource().Attributes().Get("service.name")
	if !ok || service.Str() != "apache" {
		t.Fatalf("first service = %q, want apache", service.Str())
	}
	record := first.ScopeLogs().At(0).LogRecords().At(0)
	if got := record.Body().AsString(); got != "WARN slow" {
		t.Fatalf("first body = %q, want WARN slow", got)
	}
	if got := record.SeverityText(); got != "WARN" {
		t.Fatalf("first severity = %q, want WARN", got)
	}
}

func TestFileLogsSourceRepeatAndLimit(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "app.log"), []byte("one\ntwo\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	source, err := newFileLogsSource(dir, 10, 2, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := source.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	_, lines, _, exhausted, err := source.Next(rand.New(rand.NewSource(1)), time.Unix(10, 0))
	if err != nil {
		t.Fatal(err)
	}
	if !exhausted {
		t.Fatal("source did not report exhaustion at limit")
	}
	if lines != 5 {
		t.Fatalf("lines = %d, want 5", lines)
	}
}
