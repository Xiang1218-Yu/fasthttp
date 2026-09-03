package coalesce_test

import (
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/coalesce"
	"github.com/valyala/fasthttp/fasthttputil"
)

// ---- helpers ----

func startInmemoryServer(t *testing.T, h fasthttp.RequestHandler) (*fasthttp.Server, *fasthttputil.InmemoryListener) {
	t.Helper()
	ln := fasthttputil.NewInmemoryListener()
	srv := &fasthttp.Server{Handler: h}
	go func() {
		_ = srv.Serve(ln) //nolint:errcheck
	}()
	t.Cleanup(func() {
		_ = srv.Shutdown() //nolint:errcheck
		ln.Close()
	})
	return srv, ln
}

func inmemoryClient(ln *fasthttputil.InmemoryListener) *fasthttp.Client {
	return &fasthttp.Client{
		Dial: func(_ string) (net.Conn, error) {
			return ln.Dial()
		},
	}
}

type respSnapshot struct {
	status     int
	body       string
	xCall      string
	xFrom      string
	setCookie  string
	contentTyp string
	err        error
	elapsed    time.Duration
}

func doGet(cl *fasthttp.Client, url string, headers map[string]string) respSnapshot {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetRequestURI(url)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	s := respSnapshot{}
	start := time.Now()
	if err := cl.Do(req, resp); err != nil {
		s.err = err
		s.elapsed = time.Since(start)
		return s
	}
	s.elapsed = time.Since(start)
	s.status = resp.StatusCode()
	s.body = string(resp.Body())
	s.xCall = string(resp.Header.Peek("X-Call"))
	s.xFrom = string(resp.Header.Peek("X-From"))
	s.setCookie = string(resp.Header.Peek("Set-Cookie"))
	s.contentTyp = string(resp.Header.ContentType())
	return s
}

func waitStats(t *testing.T, c *coalesce.Coalescer, want func(coalesce.Stats) bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if want(c.Stats()) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("waitStats: %s, last stats: %+v", msg, c.Stats())
}

// ---- tests ----

// Concurrent identical requests must execute the wrapped handler once and all
// receive independent copies of the same status, headers and body.
func TestCoalesceMerge(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})

	h := func(ctx *fasthttp.RequestCtx) {
		n := calls.Add(1)
		<-release
		ctx.SetContentType("application/json")
		ctx.Response.Header.Set("X-Call", fmt.Sprint(n))
		var ck fasthttp.Cookie
		ck.SetKey("sess")
		ck.SetValue("abc")
		ctx.Response.Header.SetCookie(&ck)
		ctx.SetBodyString(`{"ok":true}`)
	}

	c := coalesce.New(h)
	_, ln := startInmemoryServer(t, c.Handler())
	cl := inmemoryClient(ln)

	const n = 6
	results := make([]respSnapshot, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			results[i] = doGet(cl, "http://example.com/resource", nil)
		}(i)
	}

	waitStats(t, c, func(s coalesce.Stats) bool {
		return s.InFlightGroups == 1 && s.WaitingRequests == n-1
	}, "all followers should be waiting")

	close(release)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("handler executed %d times, want 1", got)
	}
	for i, r := range results {
		if r.err != nil {
			t.Fatalf("request %d: %v", i, r.err)
		}
		if r.status != 200 {
			t.Fatalf("request %d: status = %d, want 200", i, r.status)
		}
		if r.body != `{"ok":true}` {
			t.Fatalf("request %d: body = %q", i, r.body)
		}
		if r.xCall != "1" {
			t.Fatalf("request %d: X-Call = %q, want %q (replayed leader response)", i, r.xCall, "1")
		}
		if r.contentTyp != "application/json" {
			t.Fatalf("request %d: content-type = %q", i, r.contentTyp)
		}
		if !strings.HasPrefix(r.setCookie, "sess=abc") {
			t.Fatalf("request %d: set-cookie = %q", i, r.setCookie)
		}
	}

	// A later wave re-executes: coalescing merges concurrent calls, it is not
	// a cache.
	r := doGet(cl, "http://example.com/resource", nil)
	if r.err != nil || r.status != 200 || r.body != `{"ok":true}` {
		t.Fatalf("second wave: %+v", r)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("handler executed %d times after second wave, want 2", got)
	}

	// A different URI is a different key.
	r = doGet(cl, "http://example.com/other?x=1", nil)
	if r.err != nil || r.status != 200 {
		t.Fatalf("different URI: %+v", r)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("handler executed %d times for different URI, want 3", got)
	}
}

// The caller chooses request headers participating in the key.
func TestCoalesceKeyHeaders(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})

	h := func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		<-release
		tenant := ctx.Request.Header.Peek("X-Tenant")
		ctx.SetBodyString("tenant-" + string(tenant))
	}

	c := coalesce.New(h, coalesce.WithKeyHeaders("X-Tenant", "x-tenant-miss"))
	_, ln := startInmemoryServer(t, c.Handler())
	cl := inmemoryClient(ln)

	urls := []struct {
		tenant string
		count  int
	}{
		{"A", 3},
		{"B", 2},
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	gotBodies := make([]string, 0, 5)
	for _, u := range urls {
		for i := 0; i < u.count; i++ {
			wg.Add(1)
			go func(tenant string) {
				defer wg.Done()
				r := doGet(cl, "http://example.com/tenant-resource", map[string]string{"X-Tenant": tenant})
				mu.Lock()
				gotBodies = append(gotBodies, r.body)
				mu.Unlock()
			}(u.tenant)
		}
	}

	waitStats(t, c, func(s coalesce.Stats) bool {
		return s.InFlightGroups == 2 && s.WaitingRequests == 3
	}, "two groups with 3 followers")

	close(release)
	wg.Wait()

	if got := calls.Load(); got != 2 {
		t.Fatalf("handler executed %d times, want 2 (one per tenant)", got)
	}
	var a, b int
	for _, body := range gotBodies {
		switch body {
		case "tenant-A":
			a++
		case "tenant-B":
			b++
		default:
			t.Fatalf("unexpected body %q", body)
		}
	}
	if a != 3 || b != 2 {
		t.Fatalf("bodies: tenant-A x%d, tenant-B x%d, want 3/2", a, b)
	}
}

// Followers that cannot get the result in time answer with the timeout response.
func TestCoalesceWaitTimeout(t *testing.T) {
	var calls atomic.Int64
	h := func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		time.Sleep(300 * time.Millisecond)
		ctx.SetBodyString("slow")
	}

	c := coalesce.New(h, coalesce.WithWaitTimeout(80*time.Millisecond))
	_, ln := startInmemoryServer(t, c.Handler())
	cl := inmemoryClient(ln)

	var wg sync.WaitGroup
	res := make([]respSnapshot, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res[i] = doGet(cl, "http://example.com/slow", nil)
		}(i)
	}
	wg.Wait()

	var ok, timeout int
	for _, r := range res {
		if r.err != nil {
			t.Fatalf("unexpected client error: %v", r.err)
		}
		switch r.status {
		case 200:
			ok++
			if r.body != "slow" {
				t.Fatalf("leader body = %q", r.body)
			}
		case 503:
			timeout++
			if !strings.Contains(r.body, "wait timeout") {
				t.Fatalf("timeout body = %q", r.body)
			}
			if r.elapsed >= 250*time.Millisecond {
				t.Fatalf("follower should fail fast, 503 took %v", r.elapsed)
			}
		default:
			t.Fatalf("unexpected status %d: %+v", r.status, r)
		}
	}
	if ok != 1 || timeout != 1 {
		t.Fatalf("200 x%d / 503 x%d, want 1/1", ok, timeout)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler executed %d times, want 1", got)
	}

	// While the leader is still running, a new request can either join a
	// fresh wave (200) or time out waiting (503): both prove the handler
	// remains reachable.
	r := doGet(cl, "http://example.com/slow", nil)
	if r.err != nil || (r.status != 503 && r.status != 200) {
		t.Fatalf("post-timeout request: %+v", r)
	}
	time.Sleep(350 * time.Millisecond)
	r = doGet(cl, "http://example.com/slow", nil)
	if r.err != nil || r.status != 200 || r.body != "slow" {
		t.Fatalf("post-completion request: %+v", r)
	}
}

// A follower whose cancel channel fires aborts without receiving the shared
// response; the leader completes and the group state is cleaned up.
func TestCoalesceAppCancel(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})

	h := func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		<-release
		ctx.SetBodyString("leader-body")
	}

	c := coalesce.New(h, coalesce.WithCancel(func(ctx *fasthttp.RequestCtx) <-chan struct{} {
		if string(ctx.Request.Header.Peek("X-Cancel")) == "1" {
			ch := make(chan struct{})
			close(ch)
			return ch
		}
		return nil
	}))
	_, ln := startInmemoryServer(t, c.Handler())
	cl := inmemoryClient(ln)

	leaderDone := make(chan respSnapshot, 1)
	go func() {
		leaderDone <- doGet(cl, "http://example.com/c", nil)
	}()
	waitStats(t, c, func(s coalesce.Stats) bool { return s.InFlightGroups == 1 }, "leader in flight")

	// Follower that is canceled immediately: it must not get the leader body.
	follower := doGet(cl, "http://example.com/c", map[string]string{"X-Cancel": "1"})
	if follower.body == "leader-body" {
		t.Fatalf("canceled follower must not receive the coalesced body, got %q", follower.body)
	}
	waitStats(t, c, func(s coalesce.Stats) bool { return s.WaitingRequests == 0 }, "canceled follower should leave")

	close(release)
	leader := <-leaderDone
	if leader.err != nil || leader.status != 200 || leader.body != "leader-body" {
		t.Fatalf("leader result: %+v", leader)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler executed %d times, want 1", got)
	}

	// Group cleaned up: a later request executes the handler again.
	r := doGet(cl, "http://example.com/c", nil)
	if r.err != nil || r.status != 200 || r.body != "leader-body" {
		t.Fatalf("post-cancel request: %+v", r)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("handler executed %d times after cleanup, want 2", got)
	}
}

// A panic in the leader: leader and waiting followers get 500, the state is
// recycled so subsequent same-key requests re-execute the handler.
func TestCoalescePanicRecovery(t *testing.T) {
	var panicCalls atomic.Int64
	gate := make(chan struct{})

	h := func(ctx *fasthttp.RequestCtx) {
		if string(ctx.Path()) == "/panic" {
			n := panicCalls.Add(1)
			if n == 1 {
				gate <- struct{}{} // signal that the panicked leader started
				<-gate             // wait until followers joined, then blow up
				panic("boom")
			}
			ctx.SetBodyString("alive")
			return
		}
		ctx.SetBodyString("other-path")
	}

	c := coalesce.New(h)
	_, ln := startInmemoryServer(t, c.Handler())
	cl := inmemoryClient(ln)

	res := make([]respSnapshot, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res[i] = doGet(cl, "http://example.com/panic", nil)
		}(i)
	}

	<-gate // leader started
	waitStats(t, c, func(s coalesce.Stats) bool {
		return s.InFlightGroups == 1 && s.WaitingRequests == 1
	}, "follower joined the panicking group")
	close(gate)
	wg.Wait()

	for i, r := range res {
		if r.err != nil {
			t.Fatalf("request %d: %v", i, r.err)
		}
		if r.status != 500 {
			t.Fatalf("request %d: status = %d, want 500", i, r.status)
		}
		if r.body != "Internal Server Error" {
			t.Fatalf("request %d: body = %q, want Internal Server Error", i, r.body)
		}
	}
	if got := panicCalls.Load(); got != 1 {
		t.Fatalf("panicked handler ran %d times, want 1", got)
	}

	// State recovered: same key executes again and succeeds.
	r := doGet(cl, "http://example.com/panic", nil)
	if r.err != nil || r.status != 200 || r.body != "alive" {
		t.Fatalf("retry after panic: %+v", r)
	}
	if got := panicCalls.Load(); got != 2 {
		t.Fatalf("handler executed %d times after recovery, want 2", got)
	}

	// The server is still alive and serving other keys.
	r = doGet(cl, "http://example.com/healthy", nil)
	if r.err != nil || r.status != 200 || r.body != "other-path" {
		t.Fatalf("server health after panic: %+v", r)
	}
}

// Routes on which the coalescing handler is not mounted keep their original
// per-request execution semantics.
func TestCoalescePlainPathsUnaffected(t *testing.T) {
	var plainCalls, coalCalls atomic.Int64
	release := make(chan struct{})

	plain := func(ctx *fasthttp.RequestCtx) {
		plainCalls.Add(1)
		ctx.SetBodyString("plain")
	}
	coal := func(ctx *fasthttp.RequestCtx) {
		coalCalls.Add(1)
		<-release
		ctx.SetBodyString("coal")
	}
	c := coalesce.New(coal)

	router := func(ctx *fasthttp.RequestCtx) {
		if string(ctx.Path()) == "/coal" {
			c.Handler()(ctx)
			return
		}
		plain(ctx)
	}
	_, ln := startInmemoryServer(t, router)
	cl := inmemoryClient(ln)

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := doGet(cl, "http://example.com/plain", nil)
			if r.err != nil || r.status != 200 || r.body != "plain" {
				t.Errorf("plain request: %+v", r)
			}
		}()
	}
	wg.Wait()
	if got := plainCalls.Load(); got != 3 {
		t.Fatalf("unwrapped path handler executed %d times, want 3 (no merging)", got)
	}

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := doGet(cl, "http://example.com/coal", nil)
			if r.err != nil || r.status != 200 || r.body != "coal" {
				t.Errorf("coal request: %+v", r)
			}
		}()
	}
	waitStats(t, c, func(s coalesce.Stats) bool {
		return s.InFlightGroups == 1 && s.WaitingRequests == 2
	}, "wrapped path should merge")
	close(release)
	wg.Wait()
	if got := coalCalls.Load(); got != 1 {
		t.Fatalf("wrapped path handler executed %d times, want 1", got)
	}
}

// Streamed (chunked) leader responses are buffered and faithfully replayed.
func TestCoalesceBodyStream(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})
	h := func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		<-release
		ctx.Response.Header.Set("X-From", "stream")
		ctx.SetBodyStream(strings.NewReader("stream-body-data"), -1)
	}

	c := coalesce.New(h)
	_, ln := startInmemoryServer(t, c.Handler())
	cl := inmemoryClient(ln)

	res := make([]respSnapshot, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res[i] = doGet(cl, "http://example.com/stream", nil)
		}(i)
	}
	waitStats(t, c, func(s coalesce.Stats) bool {
		return s.InFlightGroups == 1 && s.WaitingRequests == 1
	}, "followers waiting")
	close(release)
	wg.Wait()

	for i, r := range res {
		if r.err != nil || r.status != 200 {
			t.Fatalf("stream request %d: %+v", i, r)
		}
		if r.body != "stream-body-data" {
			t.Fatalf("stream request %d: body = %q", i, r.body)
		}
		if r.xFrom != "stream" {
			t.Fatalf("stream request %d: X-From = %q, want stream", i, r.xFrom)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler executed %d times, want 1", got)
	}
}

// Real TCP client disconnect: a follower whose peer closes the connection
// while waiting stops waiting without a configured timeout (unix MSG_PEEK).
func TestCoalescePeerCloseCancelsWaiter(t *testing.T) {
	if runtime.GOOS == "windows" || runtime.GOOS == "js" || runtime.GOOS == "wasip1" {
		t.Skip("raw-socket peer watcher is only supported on unix")
	}

	var calls atomic.Int64
	gate := make(chan struct{})
	h := func(ctx *fasthttp.RequestCtx) {
		calls.Add(1)
		<-gate
		ctx.SetBodyString("ok")
	}
	c := coalesce.New(h) // no wait timeout on purpose

	tcpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &fasthttp.Server{Handler: c.Handler()}
	go func() {
		_ = srv.Serve(tcpLn) //nolint:errcheck
	}()
	t.Cleanup(func() {
		_ = srv.Shutdown() //nolint:errcheck
		_ = tcpLn.Close()  //nolint:errcheck
	})

	leaderErr := make(chan error, 1)
	go func() {
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)
		req.SetRequestURI("http://" + tcpLn.Addr().String() + "/r")
		cl := &fasthttp.Client{}
		err := cl.DoTimeout(req, resp, 5*time.Second)
		if err == nil && resp.StatusCode() != 200 && string(resp.Body()) != "ok" {
			err = fmt.Errorf("unexpected leader response: %d %q", resp.StatusCode(), resp.Body())
		}
		leaderErr <- err
	}()
	waitStats(t, c, func(s coalesce.Stats) bool { return s.InFlightGroups == 1 }, "leader in flight")

	raw, err := net.Dial("tcp", tcpLn.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(raw, "GET /r HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	waitStats(t, c, func(s coalesce.Stats) bool { return s.WaitingRequests == 1 }, "raw follower joined")

	// Client goes away without reading the response.
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	waitStats(t, c, func(s coalesce.Stats) bool { return s.WaitingRequests == 0 }, "peer close should cancel the waiter")

	close(gate)
	select {
	case err := <-leaderErr:
		if err != nil {
			t.Fatalf("leader result: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("leader never completed")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler executed %d times, want 1", got)
	}
}
