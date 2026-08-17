package wasabi

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// CircuitState is the state of the circuit breaker.
type CircuitState int32

const (
	// CircuitClosed: requests flow normally. Failures are counted.
	CircuitClosed CircuitState = iota
	// CircuitOpen: requests fail-fast with ErrCircuitOpen. After the
	// reset timeout, a probe request is allowed (half-open).
	CircuitOpen
	// CircuitHalfOpen: a single probe request is in flight. If it
	// succeeds, the breaker closes; if it fails, it re-opens.
	CircuitHalfOpen
)

// ErrCircuitOpen is returned when the circuit breaker is open and
// requests are being fail-fasted.
var ErrCircuitOpen = errors.New("wasabi: circuit breaker open")

// CircuitBreaker is a simple, dependency-free circuit breaker for the
// Wasabi adapter. It wraps the S3API calls and fail-fasts when Wasabi
// is consistently returning errors.
//
// The breaker counts consecutive failures (not a sliding window) so
// a brief blip doesn't trip it, but a sustained outage does. Once
// open, it stays open for the reset timeout, then allows a single
// probe request. If the probe succeeds, the breaker closes; if it
// fails, the reset timer restarts.
//
// The breaker is safe for concurrent use.
type CircuitBreaker struct {
	// threshold is the consecutive failure count that opens the breaker.
	threshold int
	// resetTimeout is how long the breaker stays open before allowing
	// a probe.
	resetTimeout time.Duration

	state         atomic.Int32 // CircuitState
	failures      atomic.Int64
	openedAt      atomic.Int64 // unix nano when the breaker opened
	probeInFlight atomic.Bool
	mu            sync.Mutex // guards state transitions during probe
}

// NewCircuitBreaker returns a breaker that opens after threshold
// consecutive failures and stays open for resetTimeout before
// allowing a probe.
func NewCircuitBreaker(threshold int, resetTimeout time.Duration) *CircuitBreaker {
	if threshold < 1 {
		threshold = 10
	}
	if resetTimeout <= 0 {
		resetTimeout = 30 * time.Second
	}
	cb := &CircuitBreaker{
		threshold:    threshold,
		resetTimeout: resetTimeout,
	}
	cb.state.Store(int32(CircuitClosed))
	return cb
}

// State returns the current circuit breaker state.
func (cb *CircuitBreaker) State() CircuitState {
	return CircuitState(cb.state.Load())
}

// Allow returns nil if the request should proceed, or ErrCircuitOpen
// if the breaker is open. If the breaker is in half-open state,
// Allow returns nil for the first probe request and ErrCircuitOpen
// for concurrent requests until the probe completes.
func (cb *CircuitBreaker) Allow() error {
	state := CircuitState(cb.state.Load())
	switch state {
	case CircuitClosed:
		return nil
	case CircuitOpen:
		// Check if enough time has passed to try a probe.
		openedAt := cb.openedAt.Load()
		if time.Now().UnixNano()-openedAt < int64(cb.resetTimeout) {
			return ErrCircuitOpen
		}
		// Transition to half-open: allow one probe.
		cb.mu.Lock()
		defer cb.mu.Unlock()
		// Double-check state under lock — another goroutine may have
		// already transitioned. Do NOT recursively call Allow (that
		// would deadlock on the non-reentrant mutex).
		currentState := CircuitState(cb.state.Load())
		if currentState != CircuitOpen {
			// Another goroutine already transitioned. Handle the
			// current state inline.
			if currentState == CircuitHalfOpen {
				if cb.probeInFlight.Load() {
					return ErrCircuitOpen
				}
				cb.probeInFlight.Store(true)
				return nil
			}
			// Closed — allow the request.
			return nil
		}
		if cb.probeInFlight.Load() {
			return ErrCircuitOpen
		}
		cb.probeInFlight.Store(true)
		cb.state.Store(int32(CircuitHalfOpen))
		return nil
	case CircuitHalfOpen:
		// Only one probe at a time.
		if cb.probeInFlight.Load() {
			return ErrCircuitOpen
		}
		cb.mu.Lock()
		defer cb.mu.Unlock()
		if cb.probeInFlight.Load() {
			return ErrCircuitOpen
		}
		cb.probeInFlight.Store(true)
		return nil
	default:
		return nil
	}
}

// RecordSuccess records a successful request. If the breaker is
// half-open, the probe succeeded and the breaker closes.
func (cb *CircuitBreaker) RecordSuccess() {
	state := CircuitState(cb.state.Load())
	if state == CircuitHalfOpen {
		cb.mu.Lock()
		defer cb.mu.Unlock()
		cb.state.Store(int32(CircuitClosed))
		cb.failures.Store(0)
		cb.probeInFlight.Store(false)
		return
	}
	// In closed state, reset the failure counter on success.
	cb.failures.Store(0)
}

// RecordFailure records a failed request. If the breaker is half-open,
// the probe failed and the breaker re-opens. If closed, increment the
// failure counter and open if the threshold is reached.
func (cb *CircuitBreaker) RecordFailure() {
	state := CircuitState(cb.state.Load())
	if state == CircuitHalfOpen {
		cb.mu.Lock()
		defer cb.mu.Unlock()
		cb.state.Store(int32(CircuitOpen))
		cb.openedAt.Store(time.Now().UnixNano())
		cb.probeInFlight.Store(false)
		return
	}
	if state == CircuitClosed {
		failures := cb.failures.Add(1)
		if int(failures) >= cb.threshold {
			cb.mu.Lock()
			defer cb.mu.Unlock()
			// Double-check under lock.
			if CircuitState(cb.state.Load()) == CircuitClosed {
				cb.state.Store(int32(CircuitOpen))
				cb.openedAt.Store(time.Now().UnixNano())
			}
		}
	}
}
