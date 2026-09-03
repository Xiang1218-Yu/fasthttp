package responsecache

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valyala/fasthttp"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newCtx(method, uri string) *fasthttp.RequestCtx {
	var req fasthttp.Request
	req.Header.SetMethod(method)
	req.SetRequestURI(uri)
	var ctx fasthttp.RequestCtx
	ctx.Init(&req, nil, nil)
	return &ctx
}

func get(h fasthttp.RequestHandler, uri string) *fasthttp.RequestCtx {
	ctx := newCtx("GET", uri)
	h(ctx)
	return ctx
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Unix(1700000000, 0)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

func newTestCache(t *testing.T, cfg Config) *Cache {
	t.Helper()
	c := New(cfg)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// ---------------------------------------------------------------------------
// basic hit / miss / isolation
// ---------------------------------------------------------------------------

func TestMissThenHit(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Minute})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		ctx.SetStatusCode(200)
		ctx.SetBodyString("hello")
	})

	r1 := get(h, "/hot")
	if r1.Response.StatusCode() != 200 || string(r1.Response.Body()) != "hello" {
		t.Fatalf("unexpected miss response: %d %q", r1.Response.StatusCode(), r1.Response.Body())
	}
	r2 := get(h, "/hot")
	if string(r2.Response.Body()) != "hello" {
		t.Fatalf("unexpected hit response: %q", r2.Response.Body())
	}
	if calls.Load() != 1 {
		t.Fatalf("handler called %d times, want 1", calls.Load())
	}
	st := c.Stats()
	if st.Hits != 1 || st.Misses != 1 {
		t.Fatalf("unexpected stats: %+v", st)
	}
}

func TestCachedResponsesAreIsolated(t *testing.T) {
	c := newTestCache(t, Config{TTL: time.Minute})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("ORIGINAL")
	})

	r1 := get(h, "/iso")
	body := r1.Response.Body()
	if string(body) != "ORIGINAL" {
		t.Fatalf("unexpected body %q", body)
	}
	// Mutate the body returned to a client; it must not affect the snapshot.
	body[0] = 'X'

	r2 := get(h, "/iso")
	if string(r2.Response.Body()) != "ORIGINAL" {
		t.Fatalf("cached response not isolated, got %q", r2.Response.Body())
	}
}

func TestUncacheableMethodBypasses(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Minute})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		ctx.SetBodyString("ok")
	})

	ctx := newCtx("POST", "/items")
	h(ctx)
	ctx = newCtx("POST", "/items")
	h(ctx)
	if calls.Load() != 2 {
		t.Fatalf("POST must bypass cache, handler calls=%d", calls.Load())
	}
}

// ---------------------------------------------------------------------------
// keys: method / URI / selected headers
// ---------------------------------------------------------------------------

func TestKeyByMethodAndURI(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Minute})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		ctx.SetBodyString(string(ctx.Method()) + " " + string(ctx.Path()))
	})

	get(h, "/a")
	get(h, "/a")
	get(h, "/b")
	ctx := newCtx("HEAD", "/a")
	h(ctx)
	if calls.Load() != 3 {
		t.Fatalf("want 3 handler calls (GET /a, GET /b, HEAD /a), got %d", calls.Load())
	}
}

func TestKeyHeaders(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Minute, KeyHeaders: []string{"Authorization"}})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		ctx.SetBodyString("user=" + string(ctx.Request.Header.Peek("Authorization")))
	})

	authReq := func(user string) *fasthttp.RequestCtx {
		ctx := newCtx("GET", "/me")
		if user != "" {
			ctx.Request.Header.Set("Authorization", user)
		}
		h(ctx)
		return ctx
	}

	a1 := authReq("alice")
	a2 := authReq("alice")
	b1 := authReq("bob")

	if calls.Load() != 2 {
		t.Fatalf("want 2 handler calls for 2 auth identities, got %d", calls.Load())
	}
	if string(a1.Response.Body()) != "user=alice" || string(a2.Response.Body()) != "user=alice" {
		t.Fatalf("alice variant wrong: %q %q", a1.Response.Body(), a2.Response.Body())
	}
	if string(b1.Response.Body()) != "user=bob" {
		t.Fatalf("bob variant wrong: %q", b1.Response.Body())
	}
}

// ---------------------------------------------------------------------------
// Vary
// ---------------------------------------------------------------------------

func TestVary(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Minute})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		ctx.Response.Header.Set(fasthttp.HeaderVary, "X-Tenant")
		ctx.SetBodyString("tenant=" + string(ctx.Request.Header.Peek("X-Tenant")))
	})

	tenant := func(v string) *fasthttp.RequestCtx {
		ctx := newCtx("GET", "/r")
		if v != "" {
			ctx.Request.Header.Set("X-Tenant", v)
		}
		h(ctx)
		return ctx
	}

	a1 := tenant("a")
	a2 := tenant("a")
	b1 := tenant("b")
	n1 := tenant("")

	if string(a1.Response.Body()) != "tenant=a" || string(a2.Response.Body()) != "tenant=a" || string(b1.Response.Body()) != "tenant=b" || string(n1.Response.Body()) != "tenant=" {
		t.Fatalf("vary variants mixed: %q %q %q", a2.Response.Body(), b1.Response.Body(), n1.Response.Body())
	}
	if calls.Load() != 3 {
		t.Fatalf("want 3 handler calls for 3 variants, got %d", calls.Load())
	}
}

func TestVaryStarNotCached(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Minute})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		ctx.Response.Header.Set(fasthttp.HeaderVary, "*")
		ctx.SetBodyString("v")
	})

	get(h, "/star")
	get(h, "/star")
	if calls.Load() != 2 {
		t.Fatalf("Vary: * responses must not be cached, calls=%d", calls.Load())
	}
}

func TestConfiguredVary(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Minute, Vary: []string{"X-Lang"}})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		ctx.SetBodyString("lang=" + string(ctx.Request.Header.Peek("X-Lang")))
	})

	lang := func(v string) {
		ctx := newCtx("GET", "/l")
		ctx.Request.Header.Set("X-Lang", v)
		h(ctx)
	}
	lang("en")
	lang("en")
	lang("fr")
	if calls.Load() != 2 {
		t.Fatalf("configured vary must separate variants, calls=%d", calls.Load())
	}
}

// ---------------------------------------------------------------------------
// ETag / Last-Modified conditional requests
// ---------------------------------------------------------------------------

func TestETagConditional(t *testing.T) {
	c := newTestCache(t, Config{TTL: time.Minute})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("payload")
		ctx.Response.Header.Set(fasthttp.HeaderETag, `"v1"`)
	})

	// plain miss stores the full response.
	r := get(h, "/e")
	if string(r.Response.Body()) != "payload" {
		t.Fatalf("unexpected body %q", r.Response.Body())
	}

	// matching If-None-Match on a cache hit -> 304 with empty body.
	ctx := newCtx("GET", "/e")
	ctx.Request.Header.Set(fasthttp.HeaderIfNoneMatch, `"v1"`)
	h(ctx)
	if ctx.Response.StatusCode() != fasthttp.StatusNotModified {
		t.Fatalf("want 304, got %d", ctx.Response.StatusCode())
	}
	if len(ctx.Response.Body()) != 0 {
		t.Fatalf("304 must have no body, got %q", ctx.Response.Body())
	}
	if string(ctx.Response.Header.Peek(fasthttp.HeaderETag)) != `"v1"` {
		t.Fatalf("304 must keep ETag, got %q", ctx.Response.Header.Peek(fasthttp.HeaderETag))
	}

	// non-matching validator -> full response.
	ctx2 := newCtx("GET", "/e")
	ctx2.Request.Header.Set(fasthttp.HeaderIfNoneMatch, `"other"`)
	h(ctx2)
	if ctx2.Response.StatusCode() != 200 || string(ctx2.Response.Body()) != "payload" {
		t.Fatalf("want 200 with body, got %d %q", ctx2.Response.StatusCode(), ctx2.Response.Body())
	}
}

func TestETagConditionalOnFreshMiss(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Minute})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		ctx.SetBodyString("payload")
		ctx.Response.Header.Set(fasthttp.HeaderETag, `"v1"`)
	})

	// First request ever already carries a matching validator: the client
	// gets 304 but the full response is stored for subsequent requests.
	ctx := newCtx("GET", "/e")
	ctx.Request.Header.Set(fasthttp.HeaderIfNoneMatch, `"v1"`)
	h(ctx)
	if ctx.Response.StatusCode() != fasthttp.StatusNotModified || len(ctx.Response.Body()) != 0 {
		t.Fatalf("want 304 empty, got %d %q", ctx.Response.StatusCode(), ctx.Response.Body())
	}

	r := get(h, "/e")
	if r.Response.StatusCode() != 200 || string(r.Response.Body()) != "payload" {
		t.Fatalf("stored response must be served later, got %d %q", r.Response.StatusCode(), r.Response.Body())
	}
	if calls.Load() != 1 {
		t.Fatalf("handler must be called once, got %d", calls.Load())
	}
}

func TestLastModifiedConditional(t *testing.T) {
	lastMod := time.Unix(1700000000, 0).UTC()
	httpDate := string(fasthttp.AppendHTTPDate(nil, lastMod))
	c := newTestCache(t, Config{TTL: time.Minute})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("payload")
		ctx.Response.Header.Set(fasthttp.HeaderLastModified, httpDate)
	})

	get(h, "/lm")

	// If-Modified-Since >= Last-Modified -> 304.
	ctx := newCtx("GET", "/lm")
	ctx.Request.Header.Set(fasthttp.HeaderIfModifiedSince, httpDate)
	h(ctx)
	if ctx.Response.StatusCode() != fasthttp.StatusNotModified {
		t.Fatalf("want 304, got %d", ctx.Response.StatusCode())
	}

	// If-Modified-Since before Last-Modified -> 200.
	ctx2 := newCtx("GET", "/lm")
	ctx2.Request.Header.Set(fasthttp.HeaderIfModifiedSince, string(fasthttp.AppendHTTPDate(nil, lastMod.Add(-time.Hour))))
	h(ctx2)
	if ctx2.Response.StatusCode() != 200 || len(ctx2.Response.Body()) == 0 {
		t.Fatalf("want 200 with body, got %d", ctx2.Response.StatusCode())
	}
}

func TestAutoETag(t *testing.T) {
	c := newTestCache(t, Config{TTL: time.Minute})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("abc")
	})

	r := get(h, "/auto")
	etag := r.Response.Header.Peek(fasthttp.HeaderETag)
	if len(etag) == 0 || etag[0] != 'W' {
		t.Fatalf("expected generated weak ETag, got %q", etag)
	}

	ctx := newCtx("GET", "/auto")
	ctx.Request.Header.Set(fasthttp.HeaderIfNoneMatch, string(etag))
	h(ctx)
	if ctx.Response.StatusCode() != fasthttp.StatusNotModified {
		t.Fatalf("generated ETag must support conditional requests, got %d", ctx.Response.StatusCode())
	}
}

func TestDisableETag(t *testing.T) {
	c := newTestCache(t, Config{TTL: time.Minute, DisableETag: true})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("abc")
	})
	r := get(h, "/noetag")
	if len(r.Response.Header.Peek(fasthttp.HeaderETag)) != 0 {
		t.Fatalf("ETag generation must be disabled, got %q", r.Response.Header.Peek(fasthttp.HeaderETag))
	}
}

// ---------------------------------------------------------------------------
// Cache-Control
// ---------------------------------------------------------------------------

func TestResponseNoStore(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Minute})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		ctx.Response.Header.Set(fasthttp.HeaderCacheControl, "no-store")
		ctx.SetBodyString("secret")
	})
	get(h, "/ns")
	get(h, "/ns")
	if calls.Load() != 2 {
		t.Fatalf("no-store responses must not be cached, calls=%d", calls.Load())
	}
}

func TestResponseNoCacheRevalidates(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Hour})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		ctx.Response.Header.Set(fasthttp.HeaderCacheControl, "no-cache")
		ctx.SetBodyString("v" + strconv.FormatInt(calls.Load(), 10))
	})
	r1 := get(h, "/nc")
	r2 := get(h, "/nc")
	if calls.Load() != 2 {
		t.Fatalf("no-cache responses must revalidate every request, calls=%d", calls.Load())
	}
	if string(r1.Response.Body()) != "v1" || string(r2.Response.Body()) != "v2" {
		t.Fatalf("revalidated responses must be served, got %q %q", r1.Response.Body(), r2.Response.Body())
	}
}

func TestRequestNoCache(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Hour})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		ctx.SetBodyString("v" + strconv.FormatInt(calls.Load(), 10))
	})
	get(h, "/x")
	ctx := newCtx("GET", "/x")
	ctx.Request.Header.Set(fasthttp.HeaderCacheControl, "no-cache")
	h(ctx)
	if calls.Load() != 2 || string(ctx.Response.Body()) != "v2" {
		t.Fatalf("request no-cache must force revalidation, calls=%d body=%q", calls.Load(), ctx.Response.Body())
	}
}

func TestRequestNoStoreBypasses(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Hour})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		ctx.SetBodyString("ok")
	})
	ctx := newCtx("GET", "/x")
	ctx.Request.Header.Set(fasthttp.HeaderCacheControl, "no-store")
	h(ctx)
	r := get(h, "/x")
	if calls.Load() != 2 || c.Stats().Entries != 1 {
		t.Fatalf("no-store request must not read or populate cache, calls=%d entries=%d", calls.Load(), c.Stats().Entries)
	}
	_ = r
}

func TestMaxAgeOverridesTTL(t *testing.T) {
	clk := newFakeClock()
	c := newTestCache(t, Config{TTL: time.Hour, Now: clk.now})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(fasthttp.HeaderCacheControl, "max-age=10")
		ctx.SetBodyString("v")
	})
	get(h, "/ma")
	clk.advance(5 * time.Second)
	get(h, "/ma")
	if c.Stats().Hits != 1 {
		t.Fatalf("want fresh hit within max-age, stats=%+v", c.Stats())
	}
	clk.advance(6 * time.Second) // 11s total: stale now
	get(h, "/ma")
	if c.Stats().StaleHits != 1 {
		t.Fatalf("want stale hit after max-age, stats=%+v", c.Stats())
	}
}

func TestMustRevalidateNoStale(t *testing.T) {
	clk := newFakeClock()
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Minute, StaleWhileRevalidate: time.Hour, Now: clk.now})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		ctx.Response.Header.Set(fasthttp.HeaderCacheControl, "must-revalidate")
		ctx.SetBodyString("v" + strconv.FormatInt(calls.Load(), 10))
	})
	get(h, "/mr")
	clk.advance(2 * time.Minute)
	r := get(h, "/mr")
	if c.Stats().StaleHits != 0 {
		t.Fatalf("must-revalidate responses must never be served stale")
	}
	if string(r.Response.Body()) != "v2" {
		t.Fatalf("must-revalidate must trigger synchronous refresh, got %q", r.Response.Body())
	}
}

func TestOnlyIfCached(t *testing.T) {
	c := newTestCache(t, Config{TTL: time.Minute})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("v")
	})

	ctx := newCtx("GET", "/oic")
	ctx.Request.Header.Set(fasthttp.HeaderCacheControl, "only-if-cached")
	h(ctx)
	if ctx.Response.StatusCode() != fasthttp.StatusGatewayTimeout {
		t.Fatalf("only-if-cached without entry must be 504, got %d", ctx.Response.StatusCode())
	}

	get(h, "/oic")
	ctx2 := newCtx("GET", "/oic")
	ctx2.Request.Header.Set(fasthttp.HeaderCacheControl, "only-if-cached")
	h(ctx2)
	if ctx2.Response.StatusCode() != 200 || string(ctx2.Response.Body()) != "v" {
		t.Fatalf("only-if-cached with entry must serve cache, got %d", ctx2.Response.StatusCode())
	}
}

// ---------------------------------------------------------------------------
// TTL / stale-while-revalidate / singleflight
// ---------------------------------------------------------------------------

func TestStaleWhileRevalidate(t *testing.T) {
	clk := newFakeClock()
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Minute, StaleWhileRevalidate: time.Minute, Now: clk.now})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		n := calls.Add(1)
		ctx.SetBodyString("v" + strconv.FormatInt(n, 10))
	})

	r := get(h, "/s")
	if string(r.Response.Body()) != "v1" {
		t.Fatalf("miss body=%q", r.Response.Body())
	}

	clk.advance(61 * time.Second) // stale but inside the swr window
	r2 := get(h, "/s")
	if string(r2.Response.Body()) != "v1" {
		t.Fatalf("stale body should still be v1, got %q", r2.Response.Body())
	}
	if c.Stats().StaleHits != 1 {
		t.Fatalf("want 1 stale hit, stats=%+v", c.Stats())
	}
	if !waitFor(2*time.Second, func() bool { return c.Stats().RefreshOK == 1 }) {
		t.Fatalf("background refresh did not complete, stats=%+v", c.Stats())
	}

	r3 := get(h, "/s")
	if string(r3.Response.Body()) != "v2" {
		t.Fatalf("after refresh want v2, got %q", r3.Response.Body())
	}
	if calls.Load() != 2 {
		t.Fatalf("handler calls=%d, want 2", calls.Load())
	}
}

func TestBackgroundRefreshSingleFlight(t *testing.T) {
	clk := newFakeClock()
	var calls atomic.Int64
	release := make(chan struct{})
	started := make(chan struct{}, 16)
	c := newTestCache(t, Config{TTL: time.Minute, StaleWhileRevalidate: time.Minute, Now: clk.now})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		n := calls.Add(1)
		if n > 1 {
			started <- struct{}{}
			<-release
		}
		ctx.SetBodyString("v" + strconv.FormatInt(n, 10))
	})

	get(h, "/sf")
	clk.advance(61 * time.Second)

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := newCtx("GET", "/sf")
			h(ctx)
		}()
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("background refresh never started")
	}

	// Give a potential duplicate refresh time to (wrongly) start.
	time.Sleep(100 * time.Millisecond)
	if calls.Load() != 2 {
		t.Fatalf("exactly one background refresh expected, calls=%d", calls.Load())
	}
	close(release)
	wg.Wait()

	if !waitFor(2*time.Second, func() bool { return c.Stats().RefreshOK == 1 }) {
		t.Fatalf("refresh did not complete: %+v", c.Stats())
	}
}

func TestRefreshFailureFallsBackToStale(t *testing.T) {
	clk := newFakeClock()
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Minute, StaleWhileRevalidate: time.Minute, Now: clk.now})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		n := calls.Add(1)
		if n == 1 {
			ctx.SetBodyString("good")
			return
		}
		ctx.SetStatusCode(500)
		ctx.SetBodyString("boom")
	})

	get(h, "/f")
	clk.advance(61 * time.Second)

	r2 := get(h, "/f") // stale served, background refresh fails
	if string(r2.Response.Body()) != "good" {
		t.Fatalf("stale response should be served, got %q", r2.Response.Body())
	}
	if !waitFor(2*time.Second, func() bool { return c.Stats().RefreshFailed == 1 }) {
		t.Fatalf("failed refresh not counted: %+v", c.Stats())
	}

	r3 := get(h, "/f") // still inside stale window: keep serving the old value
	if string(r3.Response.Body()) != "good" {
		t.Fatalf("failed refresh must fall back to old value, got %q", r3.Response.Body())
	}

	clk.advance(2 * time.Hour) // beyond stale window: synchronous refresh
	r4 := get(h, "/f")
	if r4.Response.StatusCode() != 500 || string(r4.Response.Body()) != "boom" {
		t.Fatalf("beyond stale window the real failure must be served, got %d %q", r4.Response.StatusCode(), r4.Response.Body())
	}
}

func TestSynchronousMissSingleFlight(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})
	started := make(chan struct{}, 16)
	c := newTestCache(t, Config{TTL: time.Minute, WaitForRefresh: 5 * time.Second})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		started <- struct{}{}
		<-release
		ctx.SetBodyString("computed")
	})

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := newCtx("GET", "/cold")
			h(ctx)
			if string(ctx.Response.Body()) != "computed" {
				t.Errorf("unexpected body %q", ctx.Response.Body())
			}
		}()
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}
	time.Sleep(100 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("exactly one synchronous computation expected, got %d", calls.Load())
	}
	close(release)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("waiters must reuse the single computation, calls=%d", calls.Load())
	}
}

func TestBackgroundRevalidationWith304(t *testing.T) {
	clk := newFakeClock()
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Minute, StaleWhileRevalidate: time.Minute, Now: clk.now})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		n := calls.Add(1)
		if n >= 2 {
			// The refresh request must carry the stored validator.
			if string(ctx.Request.Header.Peek(fasthttp.HeaderIfNoneMatch)) != `"v1"` {
				ctx.Error("missing If-None-Match", 500)
				return
			}
			ctx.SetStatusCode(fasthttp.StatusNotModified)
			ctx.Response.Header.Set(fasthttp.HeaderCacheControl, "max-age=60")
			return
		}
		ctx.SetBodyString("payload")
		ctx.Response.Header.Set(fasthttp.HeaderETag, `"v1"`)
	})

	get(h, "/rev")
	clk.advance(61 * time.Second)
	r2 := get(h, "/rev") // stale served
	if string(r2.Response.Body()) != "payload" {
		t.Fatalf("stale body=%q", r2.Response.Body())
	}
	if !waitFor(2*time.Second, func() bool { return c.Stats().Revalidated == 1 }) {
		t.Fatalf("304 revalidation did not happen: %+v", c.Stats())
	}
	r3 := get(h, "/rev")
	if string(r3.Response.Body()) != "payload" || c.Stats().Hits < 1 {
		t.Fatalf("revalidated entry must be served fresh, body=%q stats=%+v", r3.Response.Body(), c.Stats())
	}
	if calls.Load() != 2 {
		t.Fatalf("handler calls=%d, want 2", calls.Load())
	}
}

// ---------------------------------------------------------------------------
// LRU capacity
// ---------------------------------------------------------------------------

func TestLRUEviction(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Minute, MaxEntries: 2})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		ctx.SetBodyString("v")
	})
	get(h, "/1")
	get(h, "/2")
	get(h, "/3") // evicts /1
	if c.Stats().Entries != 2 {
		t.Fatalf("want 2 entries, got %d", c.Stats().Entries)
	}
	get(h, "/2") // still cached
	get(h, "/1") // evicted -> recompute
	if calls.Load() != 4 {
		t.Fatalf("LRU eviction wrong: handler calls=%d", calls.Load())
	}
}

func TestMaxBodySize(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Minute, MaxBodySize: 4})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		ctx.SetBodyString("too-large-body")
	})
	get(h, "/big")
	get(h, "/big")
	if calls.Load() != 2 {
		t.Fatalf("oversized responses must not be cached, calls=%d", calls.Load())
	}
}

// ---------------------------------------------------------------------------
// invalidation
// ---------------------------------------------------------------------------

func TestNamespaceInvalidation(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{
		TTL: time.Minute,
		NamespaceFunc: func(ctx *fasthttp.RequestCtx) []string {
			if len(ctx.Path()) >= 3 && string(ctx.Path()[:3]) == "/u/" {
				return []string{"users"}
			}
			return nil
		},
	})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		ctx.SetBodyString("v" + strconv.FormatInt(calls.Load(), 10))
	})

	get(h, "/u/1")
	get(h, "/u/2")
	get(h, "/other")
	if calls.Load() != 3 {
		t.Fatalf("calls=%d", calls.Load())
	}
	removed := c.InvalidateNamespace("users")
	if removed != 2 {
		t.Fatalf("want 2 entries removed, got %d", removed)
	}
	get(h, "/u/1")
	get(h, "/u/2")
	r := get(h, "/other") // untouched namespace
	if calls.Load() != 5 {
		t.Fatalf("after invalidation calls=%d, want 5", calls.Load())
	}
	if string(r.Response.Body()) != "v3" {
		t.Fatalf("/other should stay cached, body=%q", r.Response.Body())
	}
}

func TestInvalidateIf(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Minute})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		if string(ctx.Path()) == "/missing" {
			ctx.SetStatusCode(404)
			ctx.SetBodyString("not found")
			return
		}
		ctx.SetBodyString("ok")
	})
	get(h, "/ok")
	get(h, "/missing")

	removed := c.InvalidateIf(func(m EntryMeta) bool { return m.StatusCode == 404 })
	if removed != 1 {
		t.Fatalf("want 1 removed, got %d", removed)
	}
	get(h, "/ok")
	get(h, "/missing")
	if calls.Load() != 3 {
		t.Fatalf("only the 404 entry must be recomputed, calls=%d", calls.Load())
	}
}

func TestInvalidateAll(t *testing.T) {
	c := newTestCache(t, Config{TTL: time.Minute})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("v")
	})
	get(h, "/a")
	get(h, "/b")
	if n := c.InvalidateAll(); n != 2 {
		t.Fatalf("removed=%d, want 2", n)
	}
	if c.Stats().Entries != 0 {
		t.Fatalf("entries=%d after clear", c.Stats().Entries)
	}
}

func TestAutoInvalidate(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{
		TTL:            time.Minute,
		AutoInvalidate: true,
		NamespaceFunc:  func(ctx *fasthttp.RequestCtx) []string { return []string{"users"} },
	})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		n := calls.Add(1)
		if string(ctx.Path()) == "/users-fail" {
			ctx.SetStatusCode(500)
			ctx.SetBodyString("error")
			return
		}
		ctx.SetBodyString("v" + strconv.FormatInt(n, 10))
	})

	get(h, "/users/1")   // v1 cached
	get(h, "/users/2")   // v2 cached
	post := newCtx("POST", "/users")
	h(post) // successful unsafe method -> invalidates "users"
	if post.Response.StatusCode() != 200 {
		t.Fatalf("post status=%d", post.Response.StatusCode())
	}
	r := get(h, "/users/1") // recomputed (v4)
	if string(r.Response.Body()) != "v4" {
		t.Fatalf("namespace should be invalidated by successful POST, body=%q", r.Response.Body())
	}

	// A failed unsafe method must not invalidate.
	get(h, "/users/2") // cached again (v5)
	fail := newCtx("POST", "/users-fail")
	h(fail)
	if fail.Response.StatusCode() != 500 {
		t.Fatalf("fail status=%d", fail.Response.StatusCode())
	}
	r2 := get(h, "/users/2")
	if string(r2.Response.Body()) != "v5" {
		t.Fatalf("failed POST must not invalidate cache, body=%q", r2.Response.Body())
	}
}

// ---------------------------------------------------------------------------
// runtime config / enable toggle
// ---------------------------------------------------------------------------

func TestDisabledBypasses(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Minute, Disabled: true})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		ctx.SetBodyString("v")
	})
	get(h, "/d")
	get(h, "/d")
	if calls.Load() != 2 {
		t.Fatalf("disabled cache must pass through, calls=%d", calls.Load())
	}
	if c.Enabled() {
		t.Fatal("cache should report disabled")
	}
}

func TestRuntimeEnableToggle(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Minute})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		ctx.SetBodyString("v")
	})

	c.SetEnabled(false)
	get(h, "/t")
	get(h, "/t")
	if calls.Load() != 2 {
		t.Fatalf("disabled: calls=%d", calls.Load())
	}
	c.SetEnabled(true)
	get(h, "/t")
	get(h, "/t")
	if calls.Load() != 3 {
		t.Fatalf("re-enabled cache must serve hits, calls=%d", calls.Load())
	}
}

func TestUpdateConfig(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{TTL: time.Minute, MaxEntries: 10})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		ctx.SetBodyString("v")
	})
	get(h, "/a")
	get(h, "/b")

	// Shrink capacity at runtime: entries above capacity are evicted.
	c.UpdateConfig(func(cfg *Config) {
		cfg.MaxEntries = 1
	})
	if c.Stats().Entries != 1 {
		t.Fatalf("after shrink entries=%d, want 1", c.Stats().Entries)
	}

	// Toggle ETag generation off at runtime.
	c.InvalidateAll()
	c.UpdateConfig(func(cfg *Config) {
		cfg.DisableETag = true
	})
	r := get(h, "/c")
	if len(r.Response.Header.Peek(fasthttp.HeaderETag)) != 0 {
		t.Fatalf("updated config must disable ETag, got %q", r.Response.Header.Peek(fasthttp.HeaderETag))
	}
}

// ---------------------------------------------------------------------------
// shutdown convergence
// ---------------------------------------------------------------------------

func TestCloseConvergence(t *testing.T) {
	var calls atomic.Int64
	clk := newFakeClock()
	block := make(chan struct{})
	c := New(Config{TTL: time.Minute, StaleWhileRevalidate: time.Minute, Now: clk.now, WaitForRefresh: time.Second})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		n := calls.Add(1)
		// Only background refreshes (auto-ETag -> If-None-Match) block.
		if len(ctx.Request.Header.Peek(fasthttp.HeaderIfNoneMatch)) > 0 {
			<-block
		}
		ctx.SetBodyString("v" + strconv.FormatInt(n, 10))
	})

	get(h, "/c")
	clk.advance(61 * time.Second)
	get(h, "/c") // stale + background refresh starts

	if !waitFor(2*time.Second, func() bool { return c.Stats().ActiveRefreshes == 1 }) {
		t.Fatalf("refresh did not start: %+v", c.Stats())
	}

	// Close while a refresh is in flight: bounded wait returns with timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	if err := c.CloseWithContext(ctx); err == nil {
		cancel()
		t.Fatal("CloseWithContext should time out while refresh is blocked")
	} else {
		cancel()
	}

	// After close, requests pass straight through to the handler.
	before := calls.Load()
	r := get(h, "/c")
	if calls.Load() != before+1 || string(r.Response.Body()) == "" {
		t.Fatalf("closed cache must pass through, calls=%d", calls.Load())
	}

	close(block) // refresh unblocks; cache must converge now.
	if err := c.CloseWithContext(context.Background()); err != nil {
		t.Fatalf("Close after refresh finished: %v", err)
	}
}

func TestRequestWaitConvergesAfterTimeout(t *testing.T) {
	release := make(chan struct{})
	c := newTestCache(t, Config{TTL: time.Minute, WaitForRefresh: 50 * time.Millisecond})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		<-release
		ctx.SetBodyString("late")
	})

	// Leader starts a long computation on /wait.
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		ctx := newCtx("GET", "/wait")
		h(ctx)
	}()
	time.Sleep(100 * time.Millisecond)

	// Unstick the handler shortly after the waiter is expected to give up
	// waiting on the singleflight leader.
	go func() {
		time.Sleep(300 * time.Millisecond)
		close(release)
	}()

	// A concurrent request waits only WaitForRefresh, then computes directly
	// instead of hanging on the stuck leader.
	waiter := newCtx("GET", "/wait")
	started := time.Now()
	h(waiter)
	if elapsed := time.Since(started); elapsed < 40*time.Millisecond || elapsed > 5*time.Second {
		t.Fatalf("waiter should fall back after ~WaitForRefresh, took %v", elapsed)
	}
	if string(waiter.Response.Body()) != "late" {
		t.Fatalf("waiter should compute directly after timeout, got %q", waiter.Response.Body())
	}
	<-leaderDone
}

// ---------------------------------------------------------------------------
// refresh concurrency limit
// ---------------------------------------------------------------------------

func TestRefreshConcurrencyLimit(t *testing.T) {
	clk := newFakeClock()
	var calls, active, maxActive atomic.Int64
	gate := make(chan struct{})
	c := newTestCache(t, Config{
		TTL:                 time.Minute,
		StaleWhileRevalidate: time.Minute,
		RefreshConcurrency:  1,
		Now:                 clk.now,
	})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		// Background refreshes carry the auto-generated ETag validator.
		if len(ctx.Request.Header.Peek(fasthttp.HeaderIfNoneMatch)) > 0 {
			cur := active.Add(1)
			for {
				m := maxActive.Load()
				if cur <= m || maxActive.CompareAndSwap(m, cur) {
					break
				}
			}
			<-gate
			active.Add(-1)
		}
		ctx.SetBodyString("v")
	})

	get(h, "/a")
	get(h, "/b")
	clk.advance(61 * time.Second)
	get(h, "/a") // queues refresh for /a
	get(h, "/b") // queues refresh for /b

	if !waitFor(2*time.Second, func() bool { return active.Load() == 1 }) {
		t.Fatalf("first refresh never became active: %+v", c.Stats())
	}
	time.Sleep(150 * time.Millisecond)
	if maxActive.Load() != 1 {
		t.Fatalf("refresh concurrency exceeded limit: max=%d", maxActive.Load())
	}
	close(gate)
	if !waitFor(3*time.Second, func() bool { return c.Stats().RefreshOK == 2 }) {
		t.Fatalf("refreshes did not drain: %+v", c.Stats())
	}
}

// ---------------------------------------------------------------------------
// status header / misc
// ---------------------------------------------------------------------------

func TestXCacheHeader(t *testing.T) {
	c := newTestCache(t, Config{TTL: time.Minute, AddStatusHeader: true})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("v")
	})
	r1 := get(h, "/x")
	if string(r1.Response.Header.Peek("X-Cache")) != xCacheMiss {
		t.Fatalf("want X-Cache: MISS, got %q", r1.Response.Header.Peek("X-Cache"))
	}
	r2 := get(h, "/x")
	if string(r2.Response.Header.Peek("X-Cache")) != xCacheHit {
		t.Fatalf("want X-Cache: HIT, got %q", r2.Response.Header.Peek("X-Cache"))
	}
}

func TestCacheableResponseHook(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{
		TTL: time.Minute,
		CacheableResponse: func(ctx *fasthttp.RequestCtx, status int) bool {
			return status == 200 && string(ctx.Path()) == "/cacheme"
		},
	})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		ctx.SetBodyString("v")
	})
	get(h, "/cacheme")
	get(h, "/cacheme")
	get(h, "/ignore")
	get(h, "/ignore")
	if calls.Load() != 3 {
		t.Fatalf("hook should allow only /cacheme to be cached, calls=%d", calls.Load())
	}
}

func TestKeyFunc(t *testing.T) {
	var calls atomic.Int64
	c := newTestCache(t, Config{
		TTL: time.Minute,
		KeyFunc: func(ctx *fasthttp.RequestCtx) string {
			// Ignore query string: all variants share one entry.
			return string(ctx.Path())
		},
	})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		ctx.SetBodyString("v")
	})
	get(h, "/p?a=1")
	get(h, "/p?a=2")
	if calls.Load() != 1 {
		t.Fatalf("KeyFunc should collapse keys, calls=%d", calls.Load())
	}
	if c.BaseKey(newCtx("GET", "/p?x=1")) == "" {
		t.Fatal("BaseKey should be computed")
	}
}

// ---------------------------------------------------------------------------
// concurrent stress (race detector)
// ---------------------------------------------------------------------------

func TestConcurrentStress(t *testing.T) {
	clk := newFakeClock()
	c := newTestCache(t, Config{TTL: 50 * time.Millisecond, StaleWhileRevalidate: time.Hour, MaxEntries: 128, Now: clk.now})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("v" + string(ctx.Path()))
	})

	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := range 100 {
				uri := fmt.Sprintf("/path/%d", j%20)
				ctx := newCtx("GET", uri)
				h(ctx)
				if ctx.Response.StatusCode() >= 500 && ctx.Response.StatusCode() != fasthttp.StatusGatewayTimeout {
					t.Errorf("unexpected status %d", ctx.Response.StatusCode())
				}
				if j%7 == 0 {
					clk.advance(time.Millisecond)
				}
				if j%31 == 0 {
					c.InvalidateIf(func(m EntryMeta) bool { return false })
				}
			}
		}(i)
	}
	wg.Wait()
	_ = c.Close()
}
