package wasabi

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCircuitBreakerStartsClosed(t *testing.T) {
	cb := NewCircuitBreaker(3, 50*time.Millisecond)
	if cb.State() != CircuitClosed {
		t.Errorf("initial state = %v, want CircuitClosed", cb.State())
	}
}

func TestCircuitBreakerOpensAfterThreshold(t *testing.T) {
	cb := NewCircuitBreaker(3, 50*time.Millisecond)
	// Two failures should not open the breaker.
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != CircuitClosed {
		t.Errorf("state after 2 failures = %v, want CircuitClosed", cb.State())
	}
	// Third failure opens it.
	cb.RecordFailure()
	if cb.State() != CircuitOpen {
		t.Errorf("state after 3 failures = %v, want CircuitOpen", cb.State())
	}
}

func TestCircuitBreakerFailsFastWhenOpen(t *testing.T) {
	cb := NewCircuitBreaker(1, 50*time.Millisecond)
	cb.RecordFailure()
	if cb.State() != CircuitOpen {
		t.Fatalf("state = %v, want CircuitOpen", cb.State())
	}
	if err := cb.Allow(); !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("Allow() when open = %v, want ErrCircuitOpen", err)
	}
}

func TestCircuitBreakerHalfOpenAfterTimeout(t *testing.T) {
	cb := NewCircuitBreaker(1, 20*time.Millisecond)
	cb.RecordFailure()
	if cb.State() != CircuitOpen {
		t.Fatalf("state = %v, want CircuitOpen", cb.State())
	}
	// Wait for reset timeout.
	time.Sleep(30 * time.Millisecond)
	if err := cb.Allow(); err != nil {
		t.Errorf("Allow() after timeout = %v, want nil (should allow probe)", err)
	}
	if cb.State() != CircuitHalfOpen {
		t.Errorf("state after probe allowed = %v, want CircuitHalfOpen", cb.State())
	}
}

func TestCircuitBreakerClosesOnProbeSuccess(t *testing.T) {
	cb := NewCircuitBreaker(1, 20*time.Millisecond)
	cb.RecordFailure()
	time.Sleep(30 * time.Millisecond)
	_ = cb.Allow() // transition to half-open
	cb.RecordSuccess()
	if cb.State() != CircuitClosed {
		t.Errorf("state after probe success = %v, want CircuitClosed", cb.State())
	}
}

func TestCircuitBreakerReopensOnProbeFailure(t *testing.T) {
	cb := NewCircuitBreaker(1, 20*time.Millisecond)
	cb.RecordFailure()
	time.Sleep(30 * time.Millisecond)
	_ = cb.Allow() // transition to half-open
	cb.RecordFailure()
	if cb.State() != CircuitOpen {
		t.Errorf("state after probe failure = %v, want CircuitOpen", cb.State())
	}
}

func TestCircuitBreakerSuccessResetsFailureCount(t *testing.T) {
	cb := NewCircuitBreaker(3, 50*time.Millisecond)
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordSuccess() // resets the counter
	// Now need 3 more failures to open.
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != CircuitClosed {
		t.Errorf("state after 2 failures + success + 2 failures = %v, want CircuitClosed", cb.State())
	}
	cb.RecordFailure()
	if cb.State() != CircuitOpen {
		t.Errorf("state after 3rd failure = %v, want CircuitOpen", cb.State())
	}
}

func TestCircuitBreakerConcurrentAccess(t *testing.T) {
	cb := NewCircuitBreaker(100, 10*time.Millisecond)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = cb.Allow()
			cb.RecordSuccess()
			cb.RecordFailure()
		}()
	}
	wg.Wait()
	// Just verify it doesn't deadlock or panic.
	_ = cb.State()
}

func TestCircuitBreakerOnlyOneProbeInHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(1, 20*time.Millisecond)
	cb.RecordFailure()
	time.Sleep(30 * time.Millisecond)
	// First Allow should succeed (probe).
	if err := cb.Allow(); err != nil {
		t.Fatalf("first Allow() = %v, want nil", err)
	}
	// Second Allow should fail (probe already in flight).
	if err := cb.Allow(); !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("second Allow() = %v, want ErrCircuitOpen", err)
	}
	// Complete the probe.
	cb.RecordSuccess()
	// Now Allow should succeed again (breaker is closed).
	if err := cb.Allow(); err != nil {
		t.Errorf("Allow() after probe success = %v, want nil", err)
	}
}
