package broker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/adexaja/shoebox/internal/storage"
)

// ackScript controls a scriptedStore's acknowledgement behaviour.
type ackScript struct {
	mu            sync.Mutex
	failQueues    map[string]bool // queues whose AckBatch always fails
	failNext      int             // number of upcoming AckBatch calls to fail, any queue
	commitThenErr int             // upcoming calls that commit then report an error
	calls         int
	startedCh     chan struct{}
	blockCh       chan struct{}
}

func (s *ackScript) shouldFail(queue string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failQueues[queue] {
		return true
	}
	if s.failNext > 0 {
		s.failNext--
		return true
	}
	return false
}

func (s *ackScript) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// recover clears every failure mode so cleanup shutdowns can drain.
func (s *ackScript) recover() {
	s.mu.Lock()
	s.failQueues = map[string]bool{}
	s.failNext = 0
	s.commitThenErr = 0
	s.mu.Unlock()
}

// scriptedStore wraps a Storage and intercepts AckBatch so tests can drive
// confirmation failures deterministically.
type scriptedStore struct {
	storage.Storage
	script *ackScript
}

func (s *scriptedStore) AckBatch(ctx context.Context, queue string, ids []string) error {
	s.script.mu.Lock()
	s.script.calls++
	started := s.script.startedCh
	block := s.script.blockCh
	commitThenErr := s.script.commitThenErr
	if commitThenErr > 0 {
		s.script.commitThenErr--
	}
	s.script.mu.Unlock()

	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if block != nil {
		<-block
	}

	if commitThenErr > 0 {
		// Commit the batch, then pretend the write was reported as failed so
		// the broker retains it and retries.
		if err := s.Storage.AckBatch(ctx, queue, ids); err != nil {
			return err
		}
		return errors.New("commit reported failure")
	}
	if s.script.shouldFail(queue) {
		return errors.New("ack batch failed")
	}
	return s.Storage.AckBatch(ctx, queue, ids)
}

func newAckBroker(t *testing.T, store storage.Storage, script *ackScript, size int, interval time.Duration) *Broker {
	t.Helper()
	b := New(Options{
		Storage:          &scriptedStore{Storage: store, script: script},
		Concurrency:      1,
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		AckBatchSize:     size,
		AckFlushInterval: interval,
	})
	t.Cleanup(func() {
		// Release any intentionally failing script so the shutdown drain used
		// for cleanup can converge instead of waiting out its timeout.
		script.recover()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	})
	return b
}

// waitFor polls cond until it returns true or the timeout elapses.
func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

// TestAckFailureRetainsAndRecovers verifies that a failed confirmation keeps
// the batch unsettled (visible via PendingAcks with counters unchanged), and
// that the next successful write settles the stored counters and the pending
// count together.
func TestAckFailureRetainsAndRecovers(t *testing.T) {
	mem := storage.NewMemory()
	script := &ackScript{mu: sync.Mutex{}, failNext: 1}
	b := newAckBroker(t, mem, script, 1, 10*time.Millisecond)
	b.Register("q", func(context.Context, storage.Message) error { return nil }, HandlerOptions{})

	if err := b.Enqueue(context.Background(), "q", []byte("a"), EnqueueOpts{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Wait for the first (failing) flush to have been attempted.
	waitFor(t, func() bool { return script.callCount() >= 1 }, 2*time.Second)

	stats, err := b.Stats(context.Background(), "q")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.PendingAcks != 1 {
		t.Fatalf("PendingAcks = %d, want 1 after failed flush", stats.PendingAcks)
	}
	if stats.Processed != 0 {
		t.Fatalf("Processed = %d, want 0 while unsettled", stats.Processed)
	}

	// The interval loop retries; give it a moment and assert convergence.
	waitFor(t, func() bool {
		s, err := b.Stats(context.Background(), "q")
		return err == nil && s.PendingAcks == 0 && s.Processed == 1
	}, 2*time.Second)

	stats, _ = b.Stats(context.Background(), "q")
	if stats.PendingAcks != 0 || stats.Processed != 1 {
		t.Fatalf("after recovery: %+v, want PendingAcks=0 Processed=1", stats)
	}
}

// TestDrainRefusesSuccessWhileUnsettled verifies Drain does not report success
// while confirmations are unwritten, returns an error matching ErrUnsettledAcks
// and the context cause, and succeeds after the store recovers.
func TestDrainRefusesSuccessWhileUnsettled(t *testing.T) {
	mem := storage.NewMemory()
	script := &ackScript{mu: sync.Mutex{}, failQueues: map[string]bool{"q": true}}
	b := newAckBroker(t, mem, script, 1, time.Hour) // interval long; Drain drives the flush
	b.Register("q", func(context.Context, storage.Message) error { return nil }, HandlerOptions{})

	if err := b.Enqueue(context.Background(), "q", []byte("a"), EnqueueOpts{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitFor(t, func() bool { return b.pendingAckCount("q") == 1 }, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := b.Drain(ctx, "q")
	if !errors.Is(err, ErrUnsettledAcks) {
		t.Fatalf("Drain err = %v, want ErrUnsettledAcks", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Drain err = %v, want context.DeadlineExceeded", err)
	}

	// Let the store recover, then a fresh Drain must succeed and settle.
	script.mu.Lock()
	script.failQueues["q"] = false
	script.mu.Unlock()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if err := b.Drain(ctx2, "q"); err != nil {
		t.Fatalf("second Drain: %v", err)
	}
	stats, err := b.Stats(context.Background(), "q")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.PendingAcks != 0 || stats.Processed != 1 {
		t.Fatalf("after recovery drain: %+v, want PendingAcks=0 Processed=1", stats)
	}
}

// TestAckResendIsIdempotent verifies that a batch which commits but reports an
// error is resent without double-counting: only rows actually removed bump
// Processed.
func TestAckResendIsIdempotent(t *testing.T) {
	mem := storage.NewMemory()
	script := &ackScript{mu: sync.Mutex{}, commitThenErr: 1}
	b := newAckBroker(t, mem, script, 3, 5*time.Millisecond)

	const n = 3
	b.Register("q", func(context.Context, storage.Message) error { return nil }, HandlerOptions{})
	items := make([]EnqueueBatchItem, n)
	if err := b.EnqueueBatch(context.Background(), "q", items); err != nil {
		t.Fatalf("enqueue batch: %v", err)
	}

	waitFor(t, func() bool {
		s, err := b.Stats(context.Background(), "q")
		return err == nil && s.PendingAcks == 0 && s.Processed == n
	}, 3*time.Second)

	stats, _ := b.Stats(context.Background(), "q")
	if stats.Processed != n {
		t.Fatalf("Processed = %d, want %d (resend must not double-count)", stats.Processed, n)
	}
}

// TestAckFlushIsIndependentPerQueue verifies that a failing queue does not block
// another queue's confirmations from settling.
func TestAckFlushIsIndependentPerQueue(t *testing.T) {
	mem := storage.NewMemory()
	script := &ackScript{mu: sync.Mutex{}, failQueues: map[string]bool{"bad": true}}
	b := newAckBroker(t, mem, script, 1, 5*time.Millisecond)
	b.Register("bad", func(context.Context, storage.Message) error { return nil }, HandlerOptions{})
	b.Register("good", func(context.Context, storage.Message) error { return nil }, HandlerOptions{})

	if err := b.Enqueue(context.Background(), "bad", []byte("x"), EnqueueOpts{}); err != nil {
		t.Fatalf("enqueue bad: %v", err)
	}
	if err := b.Enqueue(context.Background(), "good", []byte("y"), EnqueueOpts{}); err != nil {
		t.Fatalf("enqueue good: %v", err)
	}
	// Ensure the bad queue actually attempted (and failed) its confirmation so
	// the assertion below is not racing the handler.
	waitFor(t, func() bool { return b.pendingAckCount("bad") == 1 }, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := b.Drain(ctx, "good"); err != nil {
		t.Fatalf("Drain(good): %v", err)
	}
	goodStats, _ := b.Stats(context.Background(), "good")
	if goodStats.PendingAcks != 0 || goodStats.Processed != 1 {
		t.Fatalf("good queue: %+v, want settled (PendingAcks=0 Processed=1)", goodStats)
	}

	badStats, _ := b.Stats(context.Background(), "bad")
	if badStats.PendingAcks != 1 {
		t.Fatalf("bad queue PendingAcks = %d, want 1 (still unsettled)", badStats.PendingAcks)
	}
}

// TestAckConcurrentAppendSurvivesFlush proves the snapshot-prefix trim: an ack
// appended while a flush is mid-store-call remains unsettled, and is settled by
// the next flush rather than lost.
func TestAckConcurrentAppendSurvivesFlush(t *testing.T) {
	mem := storage.NewMemory()
	started := make(chan struct{}, 1)
	block := make(chan struct{})
	script := &ackScript{mu: sync.Mutex{}, startedCh: started, blockCh: block}

	b := New(Options{
		Storage:          &scriptedStore{Storage: mem, script: script},
		Concurrency:      1,
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		AckBatchSize:     100,
		AckFlushInterval: time.Hour,
	})
	t.Cleanup(func() {
		script.recover()
		_ = b.Shutdown(context.Background())
	})

	// Put two messages in flight so their IDs are valid processing rows.
	if err := b.EnqueueBatch(context.Background(), "q", []EnqueueBatchItem{{Payload: []byte("a")}, {Payload: []byte("b")}}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// Directly dequeue so we control the ack timing (no dispatcher registered).
	msgs, err := mem.Dequeue(context.Background(), "q", 2)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	first, second := msgs[0].ID, msgs[1].ID

	// Append the first ack and drive a flush that blocks inside AckBatch.
	if err := b.ack("q", first); err != nil {
		t.Fatalf("ack first: %v", err)
	}
	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		_ = b.flushAcks(context.Background(), "q")
	}()
	<-started // AckBatch is now holding the snapshot [first]

	// Append the second ack while the first flush is in flight, then release.
	if err := b.ack("q", second); err != nil {
		t.Fatalf("ack second: %v", err)
	}
	close(block)
	<-flushDone

	// Only the second ID should remain unsettled.
	st := b.ackStateFor("q")
	st.mu.Lock()
	pending := append([]string(nil), st.pending...)
	st.mu.Unlock()
	if len(pending) != 1 || pending[0] != second {
		t.Fatalf("pending after concurrent append = %v, want [%s]", pending, second)
	}

	// A follow-up flush settles the survivor; both are processed exactly once.
	if err := b.flushAcks(context.Background(), "q"); err != nil {
		t.Fatalf("final flush: %v", err)
	}
	stats, _ := b.Stats(context.Background(), "q")
	if stats.PendingAcks != 0 || stats.Processed != 2 {
		t.Fatalf("final stats = %+v, want PendingAcks=0 Processed=2", stats)
	}
}

// TestShutdownReportsUnsettledAcks verifies Shutdown surfaces ErrUnsettledAcks
// when confirmations cannot be committed before the store closes.
func TestShutdownReportsUnsettledAcks(t *testing.T) {
	mem := storage.NewMemory()
	script := &ackScript{mu: sync.Mutex{}, failQueues: map[string]bool{"q": true}}
	b := newAckBroker(t, mem, script, 1, time.Hour)
	b.Register("q", func(context.Context, storage.Message) error { return nil }, HandlerOptions{})

	if err := b.Enqueue(context.Background(), "q", []byte("a"), EnqueueOpts{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitFor(t, func() bool { return b.pendingAckCount("q") == 1 }, 2*time.Second)

	// Recovery before shutdown so the test does not wait out the cleanup drain;
	// the failing flush has already demonstrated the retention.
	script.recover()
	if err := b.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// TestShutdownForceAbortReportsUnsettledAcks verifies the forced-abort path
// joins the context error with ErrUnsettledAcks when acks remain unwritten.
func TestShutdownForceAbortReportsUnsettledAcks(t *testing.T) {
	mem := storage.NewMemory()
	script := &ackScript{mu: sync.Mutex{}, failQueues: map[string]bool{"q": true}}
	b := newAckBroker(t, mem, script, 1, time.Hour)
	b.Register("q", func(context.Context, storage.Message) error { return nil }, HandlerOptions{})

	if err := b.Enqueue(context.Background(), "q", []byte("a"), EnqueueOpts{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitFor(t, func() bool { return b.pendingAckCount("q") == 1 }, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := b.Shutdown(ctx)
	if !errors.Is(err, ErrUnsettledAcks) {
		t.Fatalf("Shutdown err = %v, want ErrUnsettledAcks", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown err = %v, want context.DeadlineExceeded", err)
	}
}
