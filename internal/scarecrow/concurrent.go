package scarecrow

import (
	"context"
	"fmt"
	"runtime"
	"sync"
)

// concurrencyLimit bounds how many [concurrent] operators may run a producer goroutine at once,
// across every in-flight query in the process. Without it a query with many independent
// subtrees would spawn a scanner per subtree and multiply storage pressure by the fan-out.
var concurrencySlots = newSemaphore(max(1, runtime.GOMAXPROCS(0)))

// semaphore is a non-blocking counting semaphore. It is deliberately try-only: a [concurrent]
// that cannot get a slot runs its child inline instead of waiting, so contention costs
// parallelism and never liveness.
type semaphore struct{ ch chan struct{} }

func newSemaphore(n int) *semaphore { return &semaphore{ch: make(chan struct{}, n)} }

// tryAcquire takes a slot if one is free, reporting whether it succeeded. It never blocks.
func (s *semaphore) tryAcquire() bool {
	select {
	case s.ch <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *semaphore) release() { <-s.ch }

// prefetchDepth is how many columns a producer may run ahead of its consumer. It bounds the
// operator's extra memory at depth+2 columns and provides the backpressure that keeps a fast
// producer from materializing an unbounded amount of its input.
const prefetchDepth = 4

// concurrent runs its child on its own goroutine, letting the child's work — most importantly
// its storage latency — overlap with the parent's.
//
// This is the *only* operator in the engine that starts a goroutine. Every other operator is
// single-threaded and synchronous, and nothing blocks on anything it does not own, which is
// what makes the engine deadlock-free by construction rather than by inspection. The prototype
// instead spread errgroups across its binary and function operators and spent four commits on
// the resulting deadlocks.
//
// It cannot hand out its child's borrowed column, since the child overwrites that on its next
// call, so each column is copied into a ring this operator owns. The ring is sized so a slot is
// never reused while it is still in the channel or held by the consumer.
type concurrent struct {
	input Operator
	// slots bounds concurrent producers. Injected so a test can exercise exhaustion without
	// disturbing the process-wide limiter that parallel tests share.
	slots *semaphore

	started bool
	// inline reports that no concurrency slot was available, so the child runs synchronously.
	inline bool

	ch     chan *Column
	ring   []Column
	next   int
	err    error
	cancel context.CancelFunc
	done   chan struct{}

	closeOnce sync.Once
}

func newConcurrent(input Operator) *concurrent {
	return &concurrent{input: input, slots: concurrencySlots}
}

func (o *concurrent) String() string { return fmt.Sprintf("Concurrent(%s)", o.input) }

func (o *concurrent) Children() []Operator { return []Operator{o.input} }

// Schema resolves the child's schema and then starts the producer.
//
// Starting here rather than on the first Next is what actually buys the overlap. Schemas are
// resolved bottom-up for the whole tree before any execution (§3.3), so every producer begins
// at that point and runs ahead concurrently. Waiting for Next would serialize the very case
// this operator exists for: a vector binop drains its build side completely before it ever
// calls Next on the streaming side, so the second producer would not start until the first had
// finished.
func (o *concurrent) Schema(ctx context.Context) (*Schema, error) {
	schema, err := o.input.Schema(ctx)
	if err != nil {
		return nil, err
	}

	if !o.started {
		o.start(ctx)
	}

	return schema, nil
}

// Close stops the producer and waits for it to exit, so no goroutine outlives the query.
func (o *concurrent) Close() error {
	o.closeOnce.Do(func() {
		if o.cancel != nil {
			o.cancel()
		}

		if o.done != nil {
			// Drain so a producer blocked on send can observe the cancellation and exit.
			go func() {
				for range o.ch { //nolint:revive // draining to unblock the producer
				}
			}()
			<-o.done
		}
	})

	return o.input.Close()
}

func (o *concurrent) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if !o.started {
		o.start(ctx)
	}

	if o.inline {
		return o.input.Next(ctx)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case col, ok := <-o.ch:
		if !ok {
			return nil, o.err
		}

		return col, nil
	}
}

// start launches the producer, or marks the operator inline when no slot is free.
func (o *concurrent) start(ctx context.Context) {
	o.started = true

	if !o.slots.tryAcquire() {
		o.inline = true

		return
	}

	// The ring must outlast every column in flight: at most prefetchDepth sit in the channel and
	// one is held by the consumer, so depth+2 slots are always enough.
	o.ring = make([]Column, prefetchDepth+2)
	o.ch = make(chan *Column, prefetchDepth)
	o.done = make(chan struct{})

	produceCtx, cancel := context.WithCancel(ctx)
	o.cancel = cancel

	go o.produce(produceCtx)
}

func (o *concurrent) produce(ctx context.Context) {
	defer func() {
		close(o.ch)
		o.slots.release()
		close(o.done)
	}()

	for {
		col, err := o.input.Next(ctx)
		if err != nil {
			o.err = err

			return
		}

		if col == nil {
			return
		}

		slot := &o.ring[o.next]
		o.next = (o.next + 1) % len(o.ring)
		slot.CopyFrom(col)

		select {
		case <-ctx.Done():
			o.err = ctx.Err()

			return
		case o.ch <- slot:
		}
	}
}
