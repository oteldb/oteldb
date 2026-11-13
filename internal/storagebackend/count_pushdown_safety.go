package storagebackend

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

// A source invalidates the lease before publishing data incompatible with native presence counts.
type countPushdownLeaser interface {
	countPushdownLease() (safe bool, release func())
}

type countPushdownSafety struct {
	mu       sync.Mutex
	readers  int
	drained  chan struct{}
	disabled bool
	unsafe   atomic.Bool
}

func (s *countPushdownSafety) lease() (safe bool, release func()) {
	s.mu.Lock()
	if s.disabled {
		s.mu.Unlock()
		return false, nil
	}
	if s.readers == 0 {
		s.drained = make(chan struct{})
	}
	s.readers++
	s.mu.Unlock()
	return true, func() {
		s.mu.Lock()
		s.readers--
		if s.readers == 0 {
			if s.disabled {
				s.unsafe.Store(true)
			}
			close(s.drained)
			s.drained = nil
		}
		s.mu.Unlock()
	}
}

func (s *countPushdownSafety) invalidate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.disabled = true
	drained := s.drained
	if s.readers == 0 {
		s.unsafe.Store(true)
	}
	s.mu.Unlock()
	if drained == nil {
		return nil
	}
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *countPushdownSafety) valid() bool { return !s.unsafe.Load() }

type sampleOnlyScarecrowScanner struct {
	scarecrow.Scanner
	budget *scarecrowScanner
}

func (s sampleOnlyScarecrowScanner) WithQueryBudget(ctx context.Context) context.Context {
	return s.budget.WithQueryBudget(ctx)
}
