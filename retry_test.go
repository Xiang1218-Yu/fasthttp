package fasthttp

import (
	"errors"
	"net"
	"testing"
	"time"
)

func TestExponentialBackoffDelays(t *testing.T) {
	t.Parallel()

	b := &ExponentialBackoff{
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		Multiplier:   2,
	}

	want := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		40 * time.Millisecond,
		80 * time.Millisecond,
		100 * time.Millisecond, // capped
		100 * time.Millisecond, // capped
	}
	for i, w := range want {
		if got := b.RetryDelay(nil, nil, i+1, nil); got != w {
			t.Fatalf("attempt %d: got %v, want %v", i+1, got, w)
		}
	}
}

func TestExponentialBackoffDefaults(t *testing.T) {
	t.Parallel()

	b := &ExponentialBackoff{}
	if got := b.RetryDelay(nil, nil, 1, nil); got != DefaultBackoffInitialDelay {
		t.Fatalf("attempt 1: got %v, want %v", got, DefaultBackoffInitialDelay)
	}
	if got := b.RetryDelay(nil, nil, 2, nil); got != 2*DefaultBackoffInitialDelay {
		t.Fatalf("attempt 2: got %v, want %v", got, 2*DefaultBackoffInitialDelay)
	}
	if got := b.RetryDelay(nil, nil, 100, nil); got != DefaultBackoffMaxDelay {
		t.Fatalf("capped delay: got %v, want %v", got, DefaultBackoffMaxDelay)
	}
}

func TestExponentialBackoffJitter(t *testing.T) {
	t.Parallel()

	b := &ExponentialBackoff{
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     time.Hour, // far enough not to interfere
		Multiplier:   2,
		Jitter:       0.5,
	}

	for attempt := 1; attempt <= 4; attempt++ {
		base := float64(100*time.Millisecond) * pow2(attempt-1)
		min := time.Duration(base)
		max := time.Duration(base * 1.5)
		for range 20 {
			got := b.RetryDelay(nil, nil, attempt, nil)
			if got < min || got > max {
				t.Fatalf("attempt %d: jittered delay %v outside [%v, %v]", attempt, got, min, max)
			}
		}
	}
}

func pow2(n int) float64 {
	r := 1.0
	for range n {
		r *= 2
	}
	return r
}

func TestExponentialBackoffRetryAfter(t *testing.T) {
	t.Parallel()

	// Delta-seconds take precedence over the computed delay and MaxDelay.
	resp := AcquireResponse()
	resp.Header.Set(HeaderRetryAfter, "3")
	b := &ExponentialBackoff{
		InitialDelay: time.Millisecond,
		MaxDelay:     2 * time.Millisecond,
		Multiplier:   2,
		RetryAfter:   true,
	}
	if got := b.RetryDelay(nil, resp, 1, nil); got != 3*time.Second {
		t.Fatalf("retry-after seconds: got %v, want 3s", got)
	}
	ReleaseResponse(resp)

	// HTTP-date form.
	resp = AcquireResponse()
	date := AppendHTTPDate(nil, time.Now().Add(2*time.Second))
	resp.Header.Set(HeaderRetryAfter, string(date))
	// HTTP dates have second precision, so truncation may shave up to 1s.
	got := b.RetryDelay(nil, resp, 1, nil)
	if got < 900*time.Millisecond || got > 2500*time.Millisecond {
		t.Fatalf("retry-after date: got %v, want ~2s", got)
	}
	ReleaseResponse(resp)

	// Disabled Retry-After falls back to the computed delay.
	resp = AcquireResponse()
	resp.Header.Set(HeaderRetryAfter, "3")
	bNo := &ExponentialBackoff{
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		Multiplier:   2,
	}
	if got := bNo.RetryDelay(nil, resp, 1, nil); got != 10*time.Millisecond {
		t.Fatalf("retry-after disabled: got %v, want 10ms", got)
	}
	ReleaseResponse(resp)
}

func TestParseRetryAfter(t *testing.T) {
	t.Parallel()

	if d, ok := parseRetryAfter(nil); ok {
		t.Fatalf("empty value parsed: %v", d)
	}
	if d, ok := parseRetryAfter([]byte("garbage")); ok {
		t.Fatalf("garbage value parsed: %v", d)
	}
	if d, ok := parseRetryAfter([]byte("42")); !ok || d != 42*time.Second {
		t.Fatalf("delta-seconds: got (%v, %v)", d, ok)
	}
	if d, ok := parseRetryAfter([]byte("0")); !ok || d != 0 {
		t.Fatalf("zero delta-seconds: got (%v, %v)", d, ok)
	}
	// Past dates clamp to zero.
	past := AppendHTTPDate(nil, time.Now().Add(-time.Hour))
	if d, ok := parseRetryAfter(past); !ok || d != 0 {
		t.Fatalf("past date: got (%v, %v)", d, ok)
	}
	// Future dates resolve to roughly the remaining wait.
	future := AppendHTTPDate(nil, time.Now().Add(90*time.Second))
	if d, ok := parseRetryAfter(future); !ok || d < 80*time.Second || d > 100*time.Second {
		t.Fatalf("future date: got (%v, %v)", d, ok)
	}
}

func TestRetryPolicyFunc(t *testing.T) {
	t.Parallel()

	called := false
	var f RetryPolicy = RetryPolicyFunc(func(_ *Request, _ *Response, attempt int, _ error) time.Duration {
		called = true
		if attempt != 1 {
			t.Fatalf("unexpected attempt %d", attempt)
		}
		return 7 * time.Millisecond
	})
	if d := f.RetryDelay(nil, nil, 1, nil); d != 7*time.Millisecond {
		t.Fatalf("got %v", d)
	}
	if !called {
		t.Fatal("RetryPolicyFunc not called")
	}
}

func TestRequestSetRetryPolicy(t *testing.T) {
	t.Parallel()

	req := AcquireRequest()
	defer ReleaseRequest(req)

	if req.RetryPolicy() != nil {
		t.Fatal("expected nil policy by default")
	}
	req.SetRetryPolicy(RetryPolicyFunc(func(*Request, *Response, int, error) time.Duration { return 0 }))
	if req.RetryPolicy() == nil {
		t.Fatal("override not stored")
	}
	req.Reset()
	if req.RetryPolicy() != nil {
		t.Fatal("Reset must clear the per-request policy")
	}
}

// retryDial builds a HostClient dialer that serves canned responses from
// singleReadConn, failing with an explicit error once exhausted.
func retryDial(t *testing.T, responses ...string) (func(string) (net.Conn, error), *int) {
	t.Helper()
	dials := 0
	return func(string) (net.Conn, error) {
		if dials >= len(responses) {
			return nil, errors.New("unexpected extra dial")
		}
		s := responses[dials]
		dials++
		return &singleReadConn{s: s}, nil
	}, &dials
}

func newRetryRequest(method string) *Request {
	req := AcquireRequest()
	req.SetRequestURI("http://foobar/")
	req.Header.SetMethod(method)
	return req
}

const (
	retryOK      = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"
	retryErrResp = "invalid response"
	retry503     = "HTTP/1.1 503 Service Unavailable\r\nRetry-After: 0\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
	retry429     = "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 0\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
)

func TestHostClientRetryBackoffWaitsOnError(t *testing.T) {
	t.Parallel()

	dial, dials := retryDial(t, retryErrResp, retryOK)
	var notified []time.Duration
	c := &HostClient{
		Addr:        "foobar",
		Dial:        dial,
		RetryPolicy: &ExponentialBackoff{InitialDelay: 60 * time.Millisecond, MaxDelay: time.Second, Multiplier: 2},
		RetryNotify: func(_ *Request, _ *Response, attempt int, delay time.Duration, err error) {
			if err == nil {
				t.Error("expected non-nil err on error-path notify")
			}
			if attempt != 1 {
				t.Errorf("notify attempt: got %d, want 1", attempt)
			}
			notified = append(notified, delay)
		},
	}

	req := newRetryRequest(MethodGet)
	defer ReleaseRequest(req)
	resp := AcquireResponse()
	defer ReleaseResponse(resp)

	start := time.Now()
	if err := c.Do(req, resp); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	elapsed := time.Since(start)

	if *dials != 2 {
		t.Fatalf("dials: got %d, want 2", *dials)
	}
	if resp.StatusCode() != StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode())
	}
	if elapsed < 50*time.Millisecond {
		t.Fatalf("backoff not waited, elapsed %v", elapsed)
	}
	if len(notified) != 1 || notified[0] < 50*time.Millisecond {
		t.Fatalf("notify delays: %v", notified)
	}
}

func TestHostClientRetryBackoffStatus503(t *testing.T) {
	t.Parallel()

	dial, dials := retryDial(t, retry503, retryOK)
	c := &HostClient{
		Addr:        "foobar",
		Dial:        dial,
		RetryPolicy: NewExponentialBackoff(10*time.Millisecond, 50*time.Millisecond),
	}

	req := newRetryRequest(MethodGet)
	defer ReleaseRequest(req)
	resp := AcquireResponse()
	defer ReleaseResponse(resp)

	start := time.Now()
	if err := c.Do(req, resp); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *dials != 2 {
		t.Fatalf("dials: got %d, want 2", *dials)
	}
	if resp.StatusCode() != StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode())
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("retry-after 0 should not wait, elapsed %v", elapsed)
	}
}

func TestHostClientRetryBackoffStatus429(t *testing.T) {
	t.Parallel()

	dial, dials := retryDial(t, retry429, retry429, retryOK)
	c := &HostClient{
		Addr:        "foobar",
		Dial:        dial,
		RetryPolicy: &ExponentialBackoff{InitialDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond, Multiplier: 2, RetryAfter: true},
	}

	req := newRetryRequest(MethodGet)
	defer ReleaseRequest(req)
	resp := AcquireResponse()
	defer ReleaseResponse(resp)

	if err := c.Do(req, resp); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *dials != 3 {
		t.Fatalf("dials: got %d, want 3", *dials)
	}
	if resp.StatusCode() != StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode())
	}
}

// Client-level default policy must be propagated to HostClients it creates.
func TestClientRetryBackoffStatusDefault(t *testing.T) {
	t.Parallel()

	dial, dials := retryDial(t, retry503, retryOK)
	c := &Client{
		Dial:        func(string) (net.Conn, error) { return dial("foobar") },
		RetryPolicy: NewExponentialBackoff(time.Millisecond, 5*time.Millisecond),
	}

	req := newRetryRequest(MethodGet)
	defer ReleaseRequest(req)
	resp := AcquireResponse()
	defer ReleaseResponse(resp)

	if err := c.Do(req, resp); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *dials != 2 {
		t.Fatalf("dials: got %d, want 2", *dials)
	}
	if resp.StatusCode() != StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode())
	}
}

func TestHostClientRetryBackoffNonIdempotentStatus(t *testing.T) {
	t.Parallel()

	dial, dials := retryDial(t, retry503, retryOK)
	c := &HostClient{
		Addr:        "foobar",
		Dial:        dial,
		RetryPolicy: NewExponentialBackoff(time.Millisecond, 5*time.Millisecond),
	}

	req := newRetryRequest(MethodPost)
	defer ReleaseRequest(req)
	resp := AcquireResponse()
	defer ReleaseResponse(resp)

	if err := c.Do(req, resp); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *dials != 1 {
		t.Fatalf("POST must not be retried on 503, dials: %d", *dials)
	}
	if resp.StatusCode() != StatusServiceUnavailable {
		t.Fatalf("status: got %d, want 503", resp.StatusCode())
	}
}

func TestHostClientRetryBackoffNonIdempotentError(t *testing.T) {
	t.Parallel()

	dial, dials := retryDial(t, retryErrResp, retryOK)
	c := &HostClient{
		Addr:        "foobar",
		Dial:        dial,
		RetryPolicy: NewExponentialBackoff(time.Millisecond, 5*time.Millisecond),
	}

	req := newRetryRequest(MethodPost)
	defer ReleaseRequest(req)
	resp := AcquireResponse()
	defer ReleaseResponse(resp)

	if err := c.Do(req, resp); err == nil {
		t.Fatal("expected error for non-idempotent request")
	}
	if *dials != 1 {
		t.Fatalf("POST must not be retried on error, dials: %d", *dials)
	}
}

// Without a policy, retries stay immediate (historical behavior).
func TestHostClientRetryWithoutPolicyIsImmediate(t *testing.T) {
	t.Parallel()

	dial, dials := retryDial(t, retryErrResp, retryOK)
	c := &HostClient{
		Addr: "foobar",
		Dial: dial,
	}

	req := newRetryRequest(MethodGet)
	defer ReleaseRequest(req)
	resp := AcquireResponse()
	defer ReleaseResponse(resp)

	start := time.Now()
	if err := c.Do(req, resp); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *dials != 2 {
		t.Fatalf("dials: got %d, want 2", *dials)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("retry without policy must be immediate, elapsed %v", elapsed)
	}
}

// A Retry-After delay that doesn't fit the remaining deadline must fail
// with ErrTimeout immediately instead of waiting past the deadline.
func TestHostClientRetryBackoffDeadlineRetryAfter(t *testing.T) {
	t.Parallel()

	retry503Long := "HTTP/1.1 503 Service Unavailable\r\nRetry-After: 5\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
	dial, dials := retryDial(t, retry503Long, retryOK)
	c := &HostClient{
		Addr:        "foobar",
		Dial:        dial,
		RetryPolicy: NewExponentialBackoff(time.Millisecond, 5*time.Millisecond),
	}

	req := newRetryRequest(MethodGet)
	defer ReleaseRequest(req)
	resp := AcquireResponse()
	defer ReleaseResponse(resp)

	start := time.Now()
	err := c.DoDeadline(req, resp, time.Now().Add(150*time.Millisecond))
	elapsed := time.Since(start)

	if err != ErrTimeout {
		t.Fatalf("err: got %v, want ErrTimeout", err)
	}
	if *dials != 1 {
		t.Fatalf("dials: got %d, want 1", *dials)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("client waited past deadline: %v", elapsed)
	}
}

// An exponential delay that doesn't fit the remaining deadline fails too.
func TestHostClientRetryBackoffDeadlineBackoff(t *testing.T) {
	t.Parallel()

	dial, dials := retryDial(t, retryErrResp, retryOK)
	c := &HostClient{
		Addr:        "foobar",
		Dial:        dial,
		RetryPolicy: &ExponentialBackoff{InitialDelay: time.Second, MaxDelay: time.Second, Multiplier: 2},
	}

	req := newRetryRequest(MethodGet)
	defer ReleaseRequest(req)
	resp := AcquireResponse()
	defer ReleaseResponse(resp)

	start := time.Now()
	err := c.DoDeadline(req, resp, time.Now().Add(150*time.Millisecond))
	elapsed := time.Since(start)

	if err != ErrTimeout {
		t.Fatalf("err: got %v, want ErrTimeout", err)
	}
	if *dials != 1 {
		t.Fatalf("dials: got %d, want 1", *dials)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("client waited past deadline: %v", elapsed)
	}
}

// A per-request policy overrides the client-wide default.
func TestHostClientRetryBackoffPerRequestOverride(t *testing.T) {
	t.Parallel()

	dial, dials := retryDial(t, retryErrResp, retryOK)
	var notified time.Duration
	c := &HostClient{
		Addr:        "foobar",
		Dial:        dial,
		RetryPolicy: &ExponentialBackoff{InitialDelay: 200 * time.Millisecond, MaxDelay: time.Second, Multiplier: 2},
		RetryNotify: func(_ *Request, _ *Response, _ int, delay time.Duration, _ error) {
			notified = delay
		},
	}

	req := newRetryRequest(MethodGet)
	defer ReleaseRequest(req)
	req.SetRetryPolicy(RetryPolicyFunc(func(*Request, *Response, int, error) time.Duration {
		return time.Millisecond
	}))
	resp := AcquireResponse()
	defer ReleaseResponse(resp)

	start := time.Now()
	if err := c.Do(req, resp); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *dials != 2 {
		t.Fatalf("dials: got %d, want 2", *dials)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("per-request override not used, elapsed %v", elapsed)
	}
	if notified != time.Millisecond {
		t.Fatalf("notified delay: got %v, want 1ms (override)", notified)
	}
}

// RetryNotify observes status-driven retries with nil error.
func TestHostClientRetryNotifyStatusRetry(t *testing.T) {
	t.Parallel()

	dial, dials := retryDial(t, retry503, retryOK)
	type notifyEvent struct {
		attempt int
		delay   time.Duration
		err     error
	}
	var events []notifyEvent
	c := &HostClient{
		Addr:        "foobar",
		Dial:        dial,
		RetryPolicy: NewExponentialBackoff(time.Millisecond, 5*time.Millisecond),
		RetryNotify: func(_ *Request, _ *Response, attempt int, delay time.Duration, err error) {
			events = append(events, notifyEvent{attempt, delay, err})
		},
	}

	req := newRetryRequest(MethodGet)
	defer ReleaseRequest(req)
	resp := AcquireResponse()
	defer ReleaseResponse(resp)

	if err := c.Do(req, resp); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *dials != 2 {
		t.Fatalf("dials: got %d, want 2", *dials)
	}
	if len(events) != 1 {
		t.Fatalf("events: got %d, want 1", len(events))
	}
	if events[0].attempt != 1 || events[0].err != nil {
		t.Fatalf("unexpected event: %+v", events[0])
	}
}

// Running out of attempts on a status retry leaves the response to the caller.
func TestHostClientRetryBackoffStatusAttemptsExhausted(t *testing.T) {
	t.Parallel()

	dial, dials := retryDial(t, retry503, retry503)
	c := &HostClient{
		Addr:                      "foobar",
		Dial:                      dial,
		MaxIdemponentCallAttempts: 2,
		RetryPolicy:               NewExponentialBackoff(time.Millisecond, 5*time.Millisecond),
	}

	req := newRetryRequest(MethodGet)
	defer ReleaseRequest(req)
	resp := AcquireResponse()
	defer ReleaseResponse(resp)

	if err := c.Do(req, resp); err != nil {
		t.Fatalf("exhausted status retries must return the response, got err: %v", err)
	}
	if *dials != 2 {
		t.Fatalf("dials: got %d, want 2", *dials)
	}
	if resp.StatusCode() != StatusServiceUnavailable {
		t.Fatalf("status: got %d, want 503", resp.StatusCode())
	}
}
