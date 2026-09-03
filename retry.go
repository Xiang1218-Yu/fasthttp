package fasthttp

import (
	"math"
	"math/rand"
	"strconv"
	"time"
)

// RetryPolicy computes the delay to wait before scheduling another retry
// attempt.
//
// A RetryPolicy is consulted only after the client has already decided to
// retry an attempt (see HostClient.RetryIfErr and the idempotent request
// protection), and additionally for retryable response status codes such
// as 429 and 503 when a policy is configured.
//
// When no RetryPolicy is configured, the client retries immediately and
// keeps the historical retry behavior, including the refusal to retry
// non-idempotent requests.
type RetryPolicy interface {
	// RetryDelay returns the duration to wait before the next attempt.
	//
	// attempt is the 1-based number of the attempt that just completed,
	// i.e. 1 before the first retry.
	//
	// err is the error returned by the last attempt; it is nil when the
	// client is about to retry a response (resp non-nil), for example an
	// HTTP 429 or 503 response carrying a Retry-After header.
	//
	// resp may be nil when the failed attempt did not produce a response.
	//
	// A zero or negative duration means the next attempt is performed
	// immediately.
	//
	// The client never waits past the request timeout or deadline: if the
	// returned delay does not fit in the remaining time, the request fails
	// with ErrTimeout without performing another attempt.
	RetryDelay(req *Request, resp *Response, attempt int, err error) time.Duration
}

// RetryPolicyFunc adapts an ordinary function to the RetryPolicy interface.
type RetryPolicyFunc func(req *Request, resp *Response, attempt int, err error) time.Duration

// RetryDelay implements RetryPolicy.
func (f RetryPolicyFunc) RetryDelay(req *Request, resp *Response, attempt int, err error) time.Duration {
	return f(req, resp, attempt, err)
}

// RetryNotifyFunc is called before the client waits for a retry.
//
// It may be used for logging and metrics: attempt is the 1-based number of
// the attempt that just completed, delay is the backoff the client is about
// to wait (zero for immediate retries), and err is the error of the last
// attempt (nil for retries triggered by a response status). resp may be nil.
//
// The callback must not modify req or resp and must return quickly; it is
// invoked while the request is in flight.
type RetryNotifyFunc func(req *Request, resp *Response, attempt int, delay time.Duration, err error)

// Default backoff parameters used by ExponentialBackoff when the
// corresponding fields are left zero.
const (
	// DefaultBackoffInitialDelay is the delay before the first retry.
	DefaultBackoffInitialDelay = 10 * time.Millisecond
	// DefaultBackoffMaxDelay caps the exponentially grown delay.
	DefaultBackoffMaxDelay = time.Second
)

// ExponentialBackoff is a RetryPolicy that grows the delay between retries
// exponentially, caps it, optionally adds random jitter and can honor the
// server-provided Retry-After response header.
//
// Zero-valued fields fall back to the defaults documented on each field,
// so &ExponentialBackoff{} is ready to use. NewExponentialBackoff is a
// convenience constructor for the common case.
type ExponentialBackoff struct {
	// InitialDelay is the delay before the first retry, i.e. after
	// attempt 1. Defaults to DefaultBackoffInitialDelay when zero.
	InitialDelay time.Duration

	// MaxDelay caps the grown delay (including jitter). Defaults to
	// DefaultBackoffMaxDelay when zero or negative.
	//
	// A server Retry-After value is not capped by MaxDelay; the request
	// deadline still bounds the total wait.
	MaxDelay time.Duration

	// Multiplier is the factor applied to the delay after every attempt.
	// Defaults to 2 when zero or negative.
	Multiplier float64

	// Jitter is the maximum random fraction added to each delay, in the
	// range [0, 1]: a value of 0.2 waits an extra uniformly distributed
	// 0-20% of the computed delay. Zero (the default) disables jitter.
	// Values above 1 are treated as 1.
	Jitter float64

	// RetryAfter enables honoring the Retry-After response header
	// (delta-seconds or HTTP date) on retryable responses such as
	// HTTP 429 or 503. When the header is present and valid it takes
	// precedence over the computed delay, but is still bounded by the
	// request deadline.
	RetryAfter bool
}

// NewExponentialBackoff returns an ExponentialBackoff with the given initial
// delay and cap and sensible defaults (multiplier 2, jitter disabled,
// Retry-After honored).
func NewExponentialBackoff(initialDelay, maxDelay time.Duration) *ExponentialBackoff {
	return &ExponentialBackoff{
		InitialDelay: initialDelay,
		MaxDelay:     maxDelay,
		Multiplier:   2,
		RetryAfter:   true,
	}
}

// RetryDelay implements RetryPolicy.
func (b *ExponentialBackoff) RetryDelay(_ *Request, resp *Response, attempt int, _ error) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	if b.RetryAfter && resp != nil {
		if d, ok := parseRetryAfter(resp.Header.Peek(HeaderRetryAfter)); ok {
			return d
		}
	}

	initial := b.InitialDelay
	if initial <= 0 {
		initial = DefaultBackoffInitialDelay
	}
	maxDelay := b.MaxDelay
	if maxDelay <= 0 {
		maxDelay = DefaultBackoffMaxDelay
	}
	multiplier := b.Multiplier
	if multiplier <= 0 {
		multiplier = 2
	}

	delay := float64(initial) * math.Pow(multiplier, float64(attempt-1))
	if math.IsInf(delay, 1) || math.IsNaN(delay) || delay > float64(maxDelay) {
		delay = float64(maxDelay)
	}

	if b.Jitter > 0 {
		jitter := b.Jitter
		if jitter > 1 {
			jitter = 1
		}
		delay *= 1 + rand.Float64()*jitter
		if delay > float64(maxDelay) {
			delay = float64(maxDelay)
		}
	}

	if delay < 0 {
		return 0
	}
	return time.Duration(delay)
}

// parseRetryAfter parses a Retry-After header value, which is either a
// non-negative number of delta-seconds or an HTTP date (RFC 7231, 7.1.3).
func parseRetryAfter(v []byte) (time.Duration, bool) {
	if len(v) == 0 {
		return 0, false
	}

	// delta-seconds
	if secs, err := strconv.Atoi(b2s(v)); err == nil {
		if secs < 0 {
			secs = 0
		}
		return time.Duration(secs) * time.Second, true
	}

	// HTTP-date
	if t, err := ParseHTTPDate(v); err == nil {
		d := time.Until(t)
		if d < 0 {
			d = 0
		}
		return d, true
	}

	return 0, false
}

// isRetryableStatus reports whether a response status may trigger a
// policy-driven retry.
func isRetryableStatus(statusCode int) bool {
	return statusCode == StatusTooManyRequests || statusCode == StatusServiceUnavailable
}
