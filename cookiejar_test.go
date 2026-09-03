package fasthttp

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/valyala/fasthttp/fasthttputil"
)

func jarTestURI(t *testing.T, url string) *URI {
	t.Helper()

	u := AcquireURI()
	if err := u.Parse(nil, []byte(url)); err != nil {
		t.Fatalf("unexpected error when parsing %q: %v", url, err)
	}
	return u
}

func jarCookie(name, value string, fns ...func(*Cookie)) *Cookie {
	c := &Cookie{}
	c.SetKey(name)
	c.SetValue(value)
	for _, fn := range fns {
		fn(c)
	}
	return c
}

func jarCookieMap(j CookieJar, u *URI) map[string]string {
	m := make(map[string]string)
	for _, c := range j.Cookies(u) {
		m[string(c.Key())] = string(c.Value())
	}
	return m
}

func jarCookieNames(j CookieJar, u *URI) []string {
	cookies := j.Cookies(u)
	names := make([]string, 0, len(cookies))
	for _, c := range cookies {
		names = append(names, string(c.Key()))
	}
	return names
}

func TestCookieJarSelection(t *testing.T) {
	t.Parallel()

	jar := NewCookieJar()

	u := jarTestURI(t, "http://www.example.com/a/b")
	jar.SetCookies(u, []*Cookie{
		jarCookie("hostonly", "1"),
		jarCookie("domain", "2", func(c *Cookie) { c.SetDomain("example.com"); c.SetPath("/") }),
		jarCookie("login", "3", func(c *Cookie) { c.SetPath("/login") }),
		jarCookie("sec", "4", func(c *Cookie) { c.SetSecure(true); c.SetPath("/") }),
	})
	ReleaseURI(u)

	// A cookie without a Path attribute on /a/b gets the default path /a.
	// A cookie without a Path attribute on /login gets the default path /.
	u = jarTestURI(t, "http://www.example.com/login")
	jar.SetCookies(u, []*Cookie{
		jarCookie("sess", "5"),
	})
	ReleaseURI(u)

	checks := []struct {
		url       string
		want      map[string]string
		wantOrder []string
	}{
		{
			url:  "http://www.example.com/a/b",
			want: map[string]string{"hostonly": "1", "domain": "2", "sess": "5"},
		},
		{
			url:  "http://www.example.com/a/child",
			want: map[string]string{"hostonly": "1", "domain": "2", "sess": "5"},
		},
		{
			url:  "http://www.example.com/",
			want: map[string]string{"domain": "2", "sess": "5"},
		},
		{
			url:  "http://www.example.com/login",
			want: map[string]string{"domain": "2", "sess": "5", "login": "3"},
			// Longer path (/login) first, then path / cookies by creation time.
			wantOrder: []string{"login", "domain", "sess"},
		},
		{
			url:  "https://www.example.com/login",
			want: map[string]string{"domain": "2", "sess": "5", "login": "3", "sec": "4"},
			// sec was created before sess; both have path /.
			wantOrder: []string{"login", "domain", "sec", "sess"},
		},
		{
			url: "http://api.example.com/",
			// Only the Domain=example.com cookie crosses hosts; host-only
			// cookies stay on www.example.com.
			want: map[string]string{"domain": "2"},
		},
		{
			url:  "http://other.com/",
			want: map[string]string{},
		},
	}

	for _, tc := range checks {
		u := jarTestURI(t, tc.url)
		got := jarCookieMap(jar, u)
		if len(got) != len(tc.want) {
			t.Errorf("url %q: got %d cookies %v, want %d %v", tc.url, len(got), got, len(tc.want), tc.want)
		}
		for k, v := range tc.want {
			if got[k] != v {
				t.Errorf("url %q: cookie %q = %q, want %q", tc.url, k, got[k], v)
			}
		}
		for k := range got {
			if _, ok := tc.want[k]; !ok {
				t.Errorf("url %q: unexpected cookie %q=%q", tc.url, k, got[k])
			}
		}
		if tc.wantOrder != nil {
			gotOrder := jarCookieNames(jar, u)
			if strings.Join(gotOrder, ",") != strings.Join(tc.wantOrder, ",") {
				t.Errorf("url %q: cookie order %v, want %v", tc.url, gotOrder, tc.wantOrder)
			}
		}
		ReleaseURI(u)
	}

	// Secure cookies must not be sent over plain http.
	u = jarTestURI(t, "http://www.example.com/")
	if got := jarCookieMap(jar, u); got["sec"] != "" {
		t.Errorf("secure cookie leaked over http: %q", got["sec"])
	}
	ReleaseURI(u)
}

func TestCookieJarUpdateAndDelete(t *testing.T) {
	t.Parallel()

	jar := NewCookieJar()

	setAt := func(url string, cookies ...*Cookie) {
		u := jarTestURI(t, url)
		jar.SetCookies(u, cookies)
		ReleaseURI(u)
	}

	setAt("http://a.com/", jarCookie("sid", "v1", func(c *Cookie) { c.SetPath("/") }))
	if jar.Len() != 1 {
		t.Fatalf("Len = %d, want 1", jar.Len())
	}

	// Re-setting the same (name, domain, path) updates the value in place.
	setAt("http://a.com/", jarCookie("sid", "v2", func(c *Cookie) { c.SetPath("/") }))
	if jar.Len() != 1 {
		t.Fatalf("Len after update = %d, want 1", jar.Len())
	}
	u := jarTestURI(t, "http://a.com/")
	if got := jarCookieMap(jar, u); got["sid"] != "v2" {
		t.Fatalf("sid = %q, want v2", got["sid"])
	}
	ReleaseURI(u)

	// Max-Age <= 0 deletes the cookie.
	setAt("http://a.com/", jarCookie("sid", "v2", func(c *Cookie) { c.SetPath("/"); c.SetMaxAge(-1) }))
	if jar.Len() != 0 {
		t.Fatalf("Len after max-age delete = %d, want 0", jar.Len())
	}

	// Expires in the past deletes the cookie, including via CookieExpireDelete.
	setAt("http://a.com/", jarCookie("sid2", "x", func(c *Cookie) { c.SetPath("/") }))
	setAt("http://a.com/", jarCookie("sid2", "", func(c *Cookie) {
		c.SetPath("/")
		c.SetExpire(CookieExpireDelete)
	}))
	if jar.Len() != 0 {
		t.Fatalf("Len after expires delete = %d, want 0", jar.Len())
	}

	// Host-only and Domain cookies with the same name on different buckets
	// are distinct entries; expiring one must not remove the other.
	setAt("http://www.a.com/", jarCookie("n", "host", func(c *Cookie) { c.SetPath("/") }))
	setAt("http://www.a.com/", jarCookie("n", "dom", func(c *Cookie) { c.SetDomain("a.com"); c.SetPath("/") }))
	if jar.Len() != 2 {
		t.Fatalf("Len = %d, want 2", jar.Len())
	}
	// Expiry without a Domain attribute removes the host-only cookie only.
	setAt("http://www.a.com/", jarCookie("n", "", func(c *Cookie) { c.SetPath("/"); c.SetExpire(CookieExpireDelete) }))
	if jar.Len() != 1 {
		t.Fatalf("Len after host-only delete = %d, want 1 (domain cookie)", jar.Len())
	}
	u = jarTestURI(t, "http://www.a.com/")
	if got := jarCookieMap(jar, u); got["n"] != "dom" {
		t.Fatalf("n = %q, want dom", got["n"])
	}
	ReleaseURI(u)

	// Reject cookies claiming domains the response host is not part of,
	// and bare suffixes such as Domain=com.
	before := jar.Len()
	setAt("http://evil.com/", jarCookie("x", "1", func(c *Cookie) { c.SetDomain("com") }))
	setAt("http://evil.com/", jarCookie("x", "1", func(c *Cookie) { c.SetDomain("other.com") }))
	if jar.Len() != before {
		t.Fatalf("Len = %d, want %d (illegal Domain attributes must be rejected)", jar.Len(), before)
	}
	u = jarTestURI(t, "http://innocent.com/")
	if len(jar.Cookies(u)) != 0 {
		t.Fatalf("rejected Domain=com cookie leaked to innocent.com: %v", jarCookieMap(jar, u))
	}
	ReleaseURI(u)
}

func TestCookieJarDumpLoad(t *testing.T) {
	t.Parallel()

	jar := NewCookieJar()
	u := jarTestURI(t, "https://secure.example.com/")
	jar.SetCookies(u, []*Cookie{
		jarCookie("persistent", "p", func(c *Cookie) { c.SetPath("/"); c.SetMaxAge(3600) }),
		jarCookie("session", "s", func(c *Cookie) { c.SetPath("/") }),
		jarCookie("secureonly", "x", func(c *Cookie) { c.SetPath("/"); c.SetSecure(true) }),
	})
	ReleaseURI(u)

	data, err := jar.Dump()
	if err != nil {
		t.Fatalf("Dump: %v", err)
	}
	if !strings.Contains(string(data), "persistent") || !strings.Contains(string(data), "session") {
		t.Fatalf("Dump data misses entries: %s", data)
	}

	restored := NewCookieJar()
	if err := restored.Load(data); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if restored.Len() != jar.Len() {
		t.Fatalf("restored Len = %d, want %d", restored.Len(), jar.Len())
	}

	u = jarTestURI(t, "https://secure.example.com/")
	got := jarCookieMap(restored, u)
	if got["persistent"] != "p" || got["session"] != "s" || got["secureonly"] != "x" {
		t.Fatalf("restored cookies mismatch: %v", got)
	}
	ReleaseURI(u)

	u = jarTestURI(t, "http://secure.example.com/")
	got = jarCookieMap(restored, u)
	if got["secureonly"] != "" {
		t.Fatalf("secure cookie restored but leaks over http: %v", got)
	}
	ReleaseURI(u)

	// Load replaces existing contents.
	if err := restored.Load([]byte(`{"entries":[]}`)); err != nil {
		t.Fatalf("Load empty: %v", err)
	}
	if restored.Len() != 0 {
		t.Fatalf("Len after empty Load = %d, want 0", restored.Len())
	}

	// Malformed data is rejected.
	if err := restored.Load([]byte("{not json")); err == nil {
		t.Fatal("Load of malformed data must return an error")
	}
}

func TestCookieJarConcurrent(t *testing.T) {
	t.Parallel()

	jar := NewCookieJar()
	u := jarTestURI(t, "http://concurrent.example.com/")

	const goroutines = 32
	const iterations = 100

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				name := fmt.Sprintf("g%d", g)
				jar.SetCookies(u, []*Cookie{
					jarCookie(name, fmt.Sprintf("%d", i), func(c *Cookie) { c.SetPath("/") }),
				})
				_ = jar.Cookies(u)
				if _, err := jar.Dump(); err != nil {
					t.Errorf("Dump: %v", err)
				}
			}
		}(g)
	}
	wg.Wait()
	ReleaseURI(u)

	if jar.Len() != goroutines {
		t.Fatalf("Len = %d, want %d", jar.Len(), goroutines)
	}
}

func startJarTestServer(t *testing.T, ln net.Listener, handler RequestHandler) {
	t.Helper()

	s := &Server{Handler: handler}
	go func() {
		_ = s.Serve(ln)
	}()
	t.Cleanup(func() {
		_ = ln.Close()
	})
}

func setServerCookie(ctx *RequestCtx, name, value string, fns ...func(*Cookie)) {
	c := AcquireCookie()
	defer ReleaseCookie(c)
	c.SetKey(name)
	c.SetValue(value)
	c.SetPath("/")
	for _, fn := range fns {
		fn(c)
	}
	ctx.Response.Header.SetCookie(c)
}

func TestClientJarSession(t *testing.T) {
	t.Parallel()

	ln := fasthttputil.NewInmemoryListener()
	startJarTestServer(t, ln, func(ctx *RequestCtx) {
		switch string(ctx.Path()) {
		case "/login":
			// Multiple Set-Cookie values on one response must all be
			// absorbed and replayed together.
			setServerCookie(ctx, "sid", "abc123")
			setServerCookie(ctx, "track", "xyz")
			ctx.SetStatusCode(StatusOK)
		case "/check":
			if string(ctx.Request.Header.Cookie("sid")) != "abc123" ||
				string(ctx.Request.Header.Cookie("track")) != "xyz" {
				ctx.SetStatusCode(StatusUnauthorized)
				return
			}
			ctx.SetStatusCode(StatusOK)
			_, _ = ctx.WriteString("ok")
		default:
			ctx.SetStatusCode(StatusNotFound)
		}
	})

	c := &Client{
		Jar: NewCookieJar(),
		Dial: func(_ string) (net.Conn, error) {
			return ln.Dial()
		},
	}

	status, _, err := c.Get(nil, "http://example.com/login")
	if err != nil || status != StatusOK {
		t.Fatalf("login: status=%d err=%v", status, err)
	}

	status, body, err := c.Get(nil, "http://example.com/check")
	if err != nil || status != StatusOK {
		t.Fatalf("check: status=%d err=%v", status, err)
	}
	if string(body) != "ok" {
		t.Fatalf("check body = %q, want ok", body)
	}
}

func TestClientJarRedirectSameDomain(t *testing.T) {
	t.Parallel()

	ln := fasthttputil.NewInmemoryListener()
	startJarTestServer(t, ln, func(ctx *RequestCtx) {
		switch string(ctx.Path()) {
		case "/start":
			setServerCookie(ctx, "red", "1")
			ctx.Redirect("/target", StatusFound)
		case "/target":
			if string(ctx.Request.Header.Cookie("red")) != "1" {
				ctx.SetStatusCode(StatusBadRequest)
				return
			}
			ctx.SetStatusCode(StatusOK)
			_, _ = ctx.WriteString("reached")
		default:
			ctx.SetStatusCode(StatusNotFound)
		}
	})

	c := &Client{
		Jar: NewCookieJar(),
		Dial: func(_ string) (net.Conn, error) {
			return ln.Dial()
		},
	}

	status, body, err := c.Get(nil, "http://example.com/start")
	if err != nil {
		t.Fatalf("redirect: %v", err)
	}
	if status != StatusOK || string(body) != "reached" {
		t.Fatalf("redirect result: status=%d body=%q", status, body)
	}
}

func TestClientJarCrossDomainRedirect(t *testing.T) {
	t.Parallel()

	lnA := fasthttputil.NewInmemoryListener()
	lnB := fasthttputil.NewInmemoryListener()

	dial := func(addr string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		switch host {
		case "a.example.com":
			return lnA.Dial()
		case "b.example.com":
			return lnB.Dial()
		default:
			return nil, fmt.Errorf("unknown host %q", addr)
		}
	}

	var bSawACookie atomic.Bool

	startJarTestServer(t, lnA, func(ctx *RequestCtx) {
		switch string(ctx.Path()) {
		case "/start":
			setServerCookie(ctx, "a-cookie", "1")
			ctx.Redirect("http://b.example.com/target", StatusFound)
		case "/check":
			if string(ctx.Request.Header.Cookie("a-cookie")) != "1" {
				ctx.SetStatusCode(StatusUnauthorized)
				return
			}
			ctx.SetStatusCode(StatusOK)
			_, _ = ctx.WriteString("a-ok")
		default:
			ctx.SetStatusCode(StatusNotFound)
		}
	})

	startJarTestServer(t, lnB, func(ctx *RequestCtx) {
		switch string(ctx.Path()) {
		case "/target":
			if len(ctx.Request.Header.Cookie("a-cookie")) != 0 {
				bSawACookie.Store(true)
			}
			setServerCookie(ctx, "b-cookie", "2")
			ctx.SetStatusCode(StatusOK)
			_, _ = ctx.WriteString("b-target")
		case "/check":
			if string(ctx.Request.Header.Cookie("b-cookie")) != "2" ||
				len(ctx.Request.Header.Cookie("a-cookie")) != 0 {
				ctx.SetStatusCode(StatusBadRequest)
				return
			}
			ctx.SetStatusCode(StatusOK)
			_, _ = ctx.WriteString("b-ok")
		default:
			ctx.SetStatusCode(StatusNotFound)
		}
	})

	c := &Client{Jar: NewCookieJar(), Dial: dial}

	status, body, err := c.Get(nil, "http://a.example.com/start")
	if err != nil {
		t.Fatalf("cross-domain redirect: %v", err)
	}
	if status != StatusOK || string(body) != "b-target" {
		t.Fatalf("cross-domain redirect result: status=%d body=%q", status, body)
	}
	if bSawACookie.Load() {
		t.Fatal("a.example.com cookie was sent to b.example.com during cross-domain redirect")
	}

	status, body, err = c.Get(nil, "http://b.example.com/check")
	if err != nil || status != StatusOK || string(body) != "b-ok" {
		t.Fatalf("b check: status=%d body=%q err=%v", status, body, err)
	}

	status, body, err = c.Get(nil, "http://a.example.com/check")
	if err != nil || status != StatusOK || string(body) != "a-ok" {
		t.Fatalf("a check: status=%d body=%q err=%v", status, body, err)
	}
}

func TestClientJarManualCookies(t *testing.T) {
	t.Parallel()

	ln := fasthttputil.NewInmemoryListener()
	startJarTestServer(t, ln, func(ctx *RequestCtx) {
		_, _ = ctx.Write(ctx.Request.Header.Peek("Cookie"))
	})

	jar := NewCookieJar()
	u := jarTestURI(t, "http://example.com/")
	jar.SetCookies(u, []*Cookie{
		jarCookie("jar-cookie", "from-jar", func(c *Cookie) { c.SetPath("/") }),
	})
	ReleaseURI(u)

	c := &Client{Jar: jar, Dial: func(_ string) (net.Conn, error) { return ln.Dial() }}

	req := AcquireRequest()
	resp := AcquireResponse()
	defer ReleaseRequest(req)
	defer ReleaseResponse(resp)

	req.SetRequestURI("http://example.com/echo")
	req.Header.SetCookie("manual-cookie", "m")
	// Manual value for a name the jar also knows must win.
	req.Header.SetCookie("jar-cookie", "manual-wins")

	if err := c.Do(req, resp); err != nil {
		t.Fatalf("Do: %v", err)
	}

	body := string(resp.Body())
	if !strings.Contains(body, "manual-cookie=m") {
		t.Fatalf("manual cookie missing from %q", body)
	}
	if !strings.Contains(body, "jar-cookie=manual-wins") {
		t.Fatalf("manual cookie value should win, got %q", body)
	}
	if strings.Contains(body, "jar-cookie=from-jar") {
		t.Fatalf("jar value overrode the manual cookie: %q", body)
	}
}

func TestClientWithoutJarKeepsLegacyBehavior(t *testing.T) {
	t.Parallel()

	ln := fasthttputil.NewInmemoryListener()
	startJarTestServer(t, ln, func(ctx *RequestCtx) {
		switch string(ctx.Path()) {
		case "/set":
			setServerCookie(ctx, "x", "1")
			ctx.SetStatusCode(StatusOK)
		case "/echo":
			_, _ = ctx.Write(ctx.Request.Header.Peek("Cookie"))
		default:
			ctx.SetStatusCode(StatusNotFound)
		}
	})

	c := &Client{Dial: func(_ string) (net.Conn, error) { return ln.Dial() }}

	if _, _, err := c.Get(nil, "http://example.com/set"); err != nil {
		t.Fatalf("set: %v", err)
	}

	// Without a jar the cookie must NOT be replayed automatically.
	status, body, err := c.Get(nil, "http://example.com/echo")
	if err != nil || status != StatusOK {
		t.Fatalf("echo: status=%d err=%v", status, err)
	}
	if len(body) != 0 {
		t.Fatalf("without Jar, Set-Cookie must not be replayed, got %q", body)
	}

	// Manual cookies keep working without a jar.
	req := AcquireRequest()
	resp := AcquireResponse()
	defer ReleaseRequest(req)
	defer ReleaseResponse(resp)
	req.SetRequestURI("http://example.com/echo")
	req.Header.SetCookie("hand", "made")
	if err := c.Do(req, resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if string(resp.Body()) != "hand=made" {
		t.Fatalf("manual cookie without jar: got %q", resp.Body())
	}
}

func TestHostClientJar(t *testing.T) {
	t.Parallel()

	ln := fasthttputil.NewInmemoryListener()
	startJarTestServer(t, ln, func(ctx *RequestCtx) {
		switch string(ctx.Path()) {
		case "/login":
			setServerCookie(ctx, "sid", "xyz")
			ctx.SetStatusCode(StatusOK)
		case "/check":
			if string(ctx.Request.Header.Cookie("sid")) != "xyz" {
				ctx.SetStatusCode(StatusUnauthorized)
				return
			}
			ctx.SetStatusCode(StatusOK)
			_, _ = ctx.WriteString("ok")
		default:
			ctx.SetStatusCode(StatusNotFound)
		}
	})

	hc := &HostClient{
		Addr: "example.com:80",
		Jar:  NewCookieJar(),
		Dial: func(_ string) (net.Conn, error) {
			return ln.Dial()
		},
	}

	status, _, err := hc.Get(nil, "http://example.com/login")
	if err != nil || status != StatusOK {
		t.Fatalf("login: status=%d err=%v", status, err)
	}

	status, body, err := hc.Get(nil, "http://example.com/check")
	if err != nil || status != StatusOK || string(body) != "ok" {
		t.Fatalf("check: status=%d body=%q err=%v", status, body, err)
	}
}

func TestClientJarConcurrentRequests(t *testing.T) {
	t.Parallel()

	ln := fasthttputil.NewInmemoryListener()
	startJarTestServer(t, ln, func(ctx *RequestCtx) {
		switch string(ctx.Path()) {
		case "/login":
			setServerCookie(ctx, "sid", "abc")
			ctx.SetStatusCode(StatusOK)
		case "/check":
			if len(ctx.Request.Header.Cookie("sid")) == 0 {
				ctx.SetStatusCode(StatusUnauthorized)
				return
			}
			ctx.SetStatusCode(StatusOK)
		default:
			ctx.SetStatusCode(StatusNotFound)
		}
	})

	c := &Client{
		Jar: NewCookieJar(),
		Dial: func(_ string) (net.Conn, error) {
			return ln.Dial()
		},
	}

	const goroutines = 16
	const iterations = 20

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if _, _, err := c.Get(nil, "http://example.com/login"); err != nil {
					t.Errorf("login: %v", err)
					return
				}
				status, _, err := c.Get(nil, "http://example.com/check")
				if err != nil || status != StatusOK {
					t.Errorf("check: status=%d err=%v", status, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	// Sanity check that the jar survived concurrent traffic in one piece.
	if got := c.Jar.(*cookieJar).Len(); got != 1 {
		t.Fatalf("jar Len = %d, want 1", got)
	}
}
