// Package coalesce provides a mountable fasthttp request handler that merges
// concurrent requests for the same idempotent resource into a single upstream
// execution ("request coalescing" / single-flight).
//
// During traffic peaks many clients may request the very same resource at the
// same time (for example an expensive, cache-missing read). Without coalescing
// every request triggers the full business computation. With the handler
// returned by New wrapping such a route, only the first ("leader") request
// runs the wrapped handler; all other requests carrying the same stable key
// ("followers") wait for its result and each receive an independent copy of
// the response: same status code, same response headers and same body, written
// to their own connections.
//
// The stable key is built, by default, from the request method and request
// URI (path plus query string); the caller additionally selects which request
// headers participate in key differentiation via WithKeyHeaders (for example
// an authorization or tenant header). The whole key computation can also be
// replaced with WithKeyFunc.
//
// Waiting followers are protected by a configurable wait timeout
// (WithWaitTimeout) and abort waiting when the request is canceled: on server
// shutdown, when a caller-supplied cancel channel fires (WithCancel), or when
// the peer connection is detected as closed (best effort, on unix sockets).
// On wait timeout the follower answers with 503 Service Unavailable by
// default; on cancellation it simply returns without producing a response,
// mirroring a client that went away.
//
// If the leader handler panics, the panic is recovered (fasthttp itself does
// not recover handler panics), the leader and all followers of that group
// receive 500 Internal Server Error, and the coalescing state is discarded
// before the result is broadcast, so subsequent same-key requests run the
// handler again instead of inheriting the failed flight.
//
// Mounting the handler on selected routes leaves every other RequestHandler
// path untouched with its original semantics:
//
//	c := coalesce.New(expensiveHandler,
//	    coalesce.WithKeyHeaders("Authorization"),
//	    coalesce.WithWaitTimeout(2*time.Second),
//	)
//	srv.Handler = c.Handler()
package coalesce

import (
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valyala/bytebufferpool"
	"github.com/valyala/fasthttp"
)

// KeyFunc builds the coalescing key from a request. Requests that produce the
// same key share a single handler execution. The returned string must be
// stable for equivalent resources. KeyFunc must not panic and must not retain
// any request bytes after returning.
type KeyFunc func(ctx *fasthttp.RequestCtx) string

// CancelFunc returns a channel that is closed when the request represented by
// ctx should stop waiting for the coalesced result (for example a per-request
// deadline tracked by the application, or an externally observed client
// disconnect). Returning nil means no application-level cancellation source.
type CancelFunc func(ctx *fasthttp.RequestCtx) <-chan struct{}

// Option configures a Coalescer.
type Option func(*Coalescer)

// WithKeyHeaders declares request header names whose values participate in the
// coalescing key, in addition to method and request URI. Header lookup is
// case-insensitive. Typical examples are "Authorization", "Accept-Language"
// or a tenant header.
func WithKeyHeaders(headers ...string) Option {
	return func(c *Coalescer) {
		c.keyHeaders = append(c.keyHeaders[:0:0], headers...)
	}
}

// WithKeyFunc replaces the default key computation (method + request URI +
// selected headers) with a custom function.
func WithKeyFunc(fn KeyFunc) Option {
	return func(c *Coalescer) {
		c.keyFunc = fn
	}
}

// WithWaitTimeout limits how long a follower request waits for the in-flight
// leader before it answers on its own. After the timeout elapses the follower
// responds with the wait-timeout response (see WithWaitTimeoutResponse), while
// the leader keeps running and serves its own client. A zero or negative
// value disables the wait timeout; followers then wait until the leader
// finishes or the request is canceled.
func WithWaitTimeout(d time.Duration) Option {
	return func(c *Coalescer) {
		c.waitTimeout = d
	}
}

// WithWaitTimeoutResponse customizes the status code and body used when a
// follower exceeds the wait timeout. Defaults to 503 Service Unavailable.
func WithWaitTimeoutResponse(statusCode int, msg string) Option {
	return func(c *Coalescer) {
		c.timeoutCode = statusCode
		c.timeoutMsg = msg
	}
}

// WithCancel supplies a per-request cancellation channel for followers. The
// function is called once per follower request; a closed returned channel
// makes the follower stop waiting and return without writing a response (the
// client is considered gone).
func WithCancel(fn CancelFunc) Option {
	return func(c *Coalescer) {
		c.cancel = fn
	}
}

// Stats describes the current coalescing activity.
type Stats struct {
	// InFlightGroups is the number of keys with a handler execution running.
	InFlightGroups int64
	// WaitingRequests is the number of follower requests currently waiting
	// for a leader result.
	WaitingRequests int64
}

// Coalescer wraps a fasthttp.RequestHandler and coalesces concurrent requests
// sharing the same key. A Coalescer is safe for concurrent use and is
// intended to be reused for the whole server lifetime.
type Coalescer struct {
	next        fasthttp.RequestHandler
	keyFunc     KeyFunc
	keyHeaders  []string
	waitTimeout time.Duration
	timeoutMsg  string
	timeoutCode int
	cancel      CancelFunc

	mu    sync.Mutex
	calls map[string]*call

	inFlight atomic.Int64
	waiting  atomic.Int64
}

// call represents a single in-flight execution for a key.
type call struct {
	done     chan struct{}
	snap     snapshot
	panicked bool
}

// snapshot is the immutable, deep-captured leader response replayed to every
// follower. All byte slices are owned by the snapshot: fasthttp recycles the
// leader's RequestCtx as soon as the handler returns, so nothing may alias
// its buffers.
type snapshot struct {
	status  int
	headers [][2][]byte
	body    []byte
}

// New returns a Coalescer wrapping next. The returned handler is mounted like
// any fasthttp.RequestHandler; routes not wrapped by it keep their original
// behavior.
func New(next fasthttp.RequestHandler, opts ...Option) *Coalescer {
	c := &Coalescer{
		next:        next,
		calls:       make(map[string]*call),
		timeoutMsg:  "request coalescing: wait timeout exceeded",
		timeoutCode: fasthttp.StatusServiceUnavailable,
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.keyFunc == nil {
		headers := c.keyHeaders
		c.keyFunc = func(ctx *fasthttp.RequestCtx) string {
			return defaultKey(ctx, headers)
		}
	}
	return c
}

// Handler returns the fasthttp.RequestHandler to mount on a server or a route.
func (c *Coalescer) Handler() fasthttp.RequestHandler {
	return c.handle
}

// Stats returns the current coalescing activity.
func (c *Coalescer) Stats() Stats {
	return Stats{
		InFlightGroups:  c.inFlight.Load(),
		WaitingRequests: c.waiting.Load(),
	}
}

// defaultKey builds: METHOD \x00 RequestURI [\x00 headerValue]...
// NUL bytes cannot appear in an HTTP method, request target or header value,
// so the composition is unambiguous.
func defaultKey(ctx *fasthttp.RequestCtx, headers []string) string {
	b := bytebufferpool.Get()
	b.B = append(b.B, ctx.Method()...)
	b.B = append(b.B, 0)
	b.B = append(b.B, ctx.RequestURI()...)
	for _, h := range headers {
		b.B = append(b.B, 0)
		b.B = append(b.B, ctx.Request.Header.Peek(h)...)
	}
	s := string(b.B)
	bytebufferpool.Put(b)
	return s
}

func (c *Coalescer) handle(ctx *fasthttp.RequestCtx) {
	key := c.keyFunc(ctx)

	c.mu.Lock()
	cl := c.calls[key]
	leader := false
	if cl == nil {
		cl = &call{done: make(chan struct{})}
		c.calls[key] = cl
		c.inFlight.Add(1)
		leader = true
	}
	c.mu.Unlock()

	if leader {
		c.runLeader(ctx, key, cl)
		return
	}
	c.runFollower(ctx, cl)
}

// runLeader executes the wrapped handler exactly once for the group, captures
// the response and broadcasts it. On panic the group state is removed before
// broadcasting so subsequent same-key requests re-execute the handler.
func (c *Coalescer) runLeader(ctx *fasthttp.RequestCtx, key string, cl *call) {
	defer func() {
		if r := recover(); r != nil {
			ctx.Logger().Printf("coalesce: recovered panic in handler for key %q: %v\n%s", key, r, debug.Stack())

			// The leader response may be half populated; replace it with a
			// clean 500 both for the leader and for the waiting followers.
			cl.panicked = true
			cl.snap = snapshot{
				status: fasthttp.StatusInternalServerError,
				body:   []byte(fasthttp.StatusMessage(fasthttp.StatusInternalServerError)),
			}
			ctx.Response.Reset()
			cl.snap.writeTo(ctx)
		}

		// State recovery: detach the finished group before broadcasting.
		// Requests arriving afterwards become leaders of a fresh execution.
		c.mu.Lock()
		if c.calls[key] == cl {
			delete(c.calls, key)
		}
		c.mu.Unlock()
		c.inFlight.Add(-1)
		close(cl.done)
	}()

	c.next(ctx)

	cl.snap = captureSnapshot(ctx)
}

// runFollower waits for the leader result, a wait timeout or cancellation and
// then either replays the shared response or answers on its own.
func (c *Coalescer) runFollower(ctx *fasthttp.RequestCtx, cl *call) {
	c.waiting.Add(1)
	defer c.waiting.Add(-1)

	var timeoutCh <-chan time.Time
	var watchDeadline time.Time
	if c.waitTimeout > 0 {
		timer := time.NewTimer(c.waitTimeout)
		defer timer.Stop()
		timeoutCh = timer.C
		watchDeadline = time.Now().Add(c.waitTimeout)
	}

	// Cancellation sources:
	//  - server shutdown (ctx.Done is closed when the server shuts down),
	//  - optional application-level cancel channel,
	//  - best-effort peer connection close detection.
	var appCancel, peerClosed <-chan struct{}
	if c.cancel != nil {
		appCancel = safeCancel(c.cancel, ctx)
	}
	stopWatch := make(chan struct{})
	defer close(stopWatch)
	peerClosed = watchConnClose(ctx.Conn(), watchDeadline, stopWatch)

	select {
	case <-cl.done:
		cl.snap.writeTo(ctx)
	case <-timeoutCh:
		ctx.Error(c.timeoutMsg, c.timeoutCode)
	case <-ctx.Done():
		// Server is shutting down; do not produce a response.
	case <-appCancel:
		// Request canceled by the application/client; do not produce a response.
	case <-peerClosed:
		// Peer connection went away; do not produce a response.
	}
}

func safeCancel(fn CancelFunc, ctx *fasthttp.RequestCtx) (ch <-chan struct{}) {
	defer func() {
		if recover() != nil {
			ch = nil
		}
	}()
	return fn(ctx)
}

// captureSnapshot deep-copies the leader response. Body streams (including
// streamed/chunked responses) are drained into a buffer; the leader keeps
// serving the drained bytes, while followers replay the captured copy.
func captureSnapshot(ctx *fasthttp.RequestCtx) snapshot {
	snap := snapshot{
		status: ctx.Response.StatusCode(),
	}

	// Body() drains and closes any body stream, leaving the bytes in the
	// leader response buffer for the leader's own write.
	body := ctx.Response.Body()
	snap.body = append(snap.body, body...)

	ctx.Response.Header.All()(func(k, v []byte) bool {
		// Content-Length is recomputed from the replayed body; Connection is
		// per-connection and managed by fasthttp; Trailer belongs to streamed
		// responses and has no meaning once the body is fully buffered.
		switch string(k) {
		case "Content-Length", "Connection", "Trailer":
			return true
		}
		snap.headers = append(snap.headers, [2][]byte{
			append([]byte(nil), k...),
			append([]byte(nil), v...),
		})
		return true
	})

	return snap
}

// writeTo replays the snapshot onto an independent follower response.
func (s snapshot) writeTo(ctx *fasthttp.RequestCtx) {
	if s.status != 0 {
		ctx.SetStatusCode(s.status)
	}
	for i := range s.headers {
		// Set copies the key/value into the response header storage; multiple
		// Set-Cookie entries are appended as separate cookies by fasthttp.
		ctx.Response.Header.Set(string(s.headers[i][0]), string(s.headers[i][1]))
	}
	if len(s.body) > 0 {
		ctx.SetBody(s.body)
	}
}
