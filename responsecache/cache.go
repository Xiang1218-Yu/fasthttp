// Package responsecache provides a mountable HTTP response cache middleware
// for fasthttp.
//
// The cache keys responses by HTTP method, request URI and caller selected
// request headers (plus the headers listed in Vary). It supports TTL based
// freshness, LRU capacity eviction, request and response Cache-Control
// directives, Vary, ETag/Last-Modified conditional requests (including
// automatic 304 responses), stale-while-revalidate with a single background
// refresh per key, a bounded refresh concurrency, batch invalidation by
// namespace or custom predicate, atomic runtime configuration updates and a
// graceful shutdown.
//
// When the cache is disabled (or a request is not cacheable) the wrapped
// handler is invoked directly with unchanged semantics.
package responsecache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valyala/fasthttp"
)

var (
	// errClosed is returned internally when the cache was closed.
	errClosed = errors.New("responsecache: cache is closed")
	// errNotStored is returned when a refresh produced a non cacheable response.
	errNotStored = errors.New("responsecache: refreshed response is not cacheable")
	// errNoHandler is returned when a refresh runs without a wrapped handler.
	errNoHandler = errors.New("responsecache: no handler configured")
)

// X-Cache status values reported when Config.AddStatusHeader is enabled.
const (
	xCacheHit    = "HIT"
	xCacheMiss   = "MISS"
	xCacheStale  = "STALE"
	xCacheBypass = "BYPASS"
)

// Config configures a Cache. The zero value is not usable; use New with at
// least TTL set (a sensible default is applied when TTL is zero).
type Config struct {
	// Disabled disables the cache. The wrapped handler is called for every
	// request. Toggle at runtime with Cache.SetEnabled.
	Disabled bool

	// TTL is the default fresh lifetime of a cached entry. Response
	// Cache-Control "max-age"/"s-maxage" and Expires take precedence.
	// Defaults to 5 minutes when zero.
	TTL time.Duration

	// StaleWhileRevalidate is the default window after expiry during which a
	// stale response may be served while a background refresh runs. Response
	// "stale-while-revalidate" takes precedence. Defaults to TTL when zero.
	StaleWhileRevalidate time.Duration

	// MaxEntries bounds the number of stored entries; least recently used
	// entries are evicted first. Zero means unlimited. Adjustable at runtime.
	MaxEntries int

	// MaxBodySize skips caching responses whose body is larger than the given
	// size. Defaults to 16 MiB when zero.
	MaxBodySize int

	// Methods lists cacheable request methods. Defaults to GET and HEAD.
	Methods []string

	// KeyHeaders lists request headers that are always included in the cache
	// key (in addition to Vary), e.g. Authorization or X-Tenant.
	KeyHeaders []string

	// Vary lists request headers enforced for every cached response, merged
	// with the response Vary header.
	Vary []string

	// KeyFunc optionally overrides the cache key computation. Returning an
	// empty key disables caching for the request.
	KeyFunc func(ctx *fasthttp.RequestCtx) string

	// NamespaceFunc tags stored entries with namespaces for batch
	// invalidation (see Cache.InvalidateNamespace).
	NamespaceFunc func(ctx *fasthttp.RequestCtx) []string

	// CacheableRequest decides whether a request may use the cache. When it
	// returns false the handler is invoked without caching.
	CacheableRequest func(ctx *fasthttp.RequestCtx) bool

	// CacheableResponse decides whether a produced response may be stored.
	// When nil, responses with status codes 200, 203, 301, 308, 404 and 410
	// are stored (subject to Cache-Control and body size).
	CacheableResponse func(ctx *fasthttp.RequestCtx, statusCode int) bool

	// DisableETag disables automatic generation of a weak ETag for cacheable
	// responses that do not already carry one. ETag generation is enabled by
	// default so conditional requests work out of the box.
	DisableETag bool

	// AddStatusHeader adds an "X-Cache: HIT|MISS|STALE|BYPASS" response header.
	AddStatusHeader bool

	// RefreshConcurrency bounds the number of background refreshes running in
	// parallel. Defaults to 64 when zero. Adjustable at runtime.
	RefreshConcurrency int

	// WaitForRefresh bounds how long a synchronous request waits for an
	// in-flight singleflight refresh before invoking the handler directly.
	// Defaults to 30 seconds when zero.
	WaitForRefresh time.Duration

	// AutoInvalidate, when true, invokes the handler for non-cacheable
	// methods (POST/PUT/PATCH/DELETE/...) and invalidates the namespaces
	// returned by NamespaceFunc after a successful (status < 400) response.
	AutoInvalidate bool

	// Now overrides the clock (mainly useful in tests).
	Now func() time.Time

	// Logger receives background refresh errors. When nil the fasthttp
	// default logger is used.
	Logger fasthttp.Logger
}

// runtimeConfig is the normalized, atomically swapped configuration used by
// the hot path.
type runtimeConfig struct {
	ttl           time.Duration
	swr           time.Duration
	maxBodySize   int
	methods       map[string]struct{}
	keyHeaders    [][]byte
	vary          [][]byte
	keyFunc       func(ctx *fasthttp.RequestCtx) string
	nsFunc        func(ctx *fasthttp.RequestCtx) []string
	cacheableReq  func(ctx *fasthttp.RequestCtx) bool
	cacheableResp func(ctx *fasthttp.RequestCtx, statusCode int) bool
	genETag       bool
	addStatusHdr  bool
	refreshConc   int
	waitRefresh   time.Duration
	autoInvalid   bool
	now           func() time.Time
	logger        fasthttp.Logger
}

func normalizeConfig(cfg Config) runtimeConfig {
	rc := runtimeConfig{
		ttl:           cfg.TTL,
		swr:           cfg.StaleWhileRevalidate,
		maxBodySize:   cfg.MaxBodySize,
		keyFunc:       cfg.KeyFunc,
		nsFunc:        cfg.NamespaceFunc,
		cacheableReq:  cfg.CacheableRequest,
		cacheableResp: cfg.CacheableResponse,
		genETag:       !cfg.DisableETag,
		addStatusHdr:  cfg.AddStatusHeader,
		waitRefresh:   cfg.WaitForRefresh,
		autoInvalid:   cfg.AutoInvalidate,
		now:           cfg.Now,
		logger:        cfg.Logger,
		refreshConc:   cfg.RefreshConcurrency,
	}
	if rc.ttl <= 0 {
		rc.ttl = 5 * time.Minute
	}
	if rc.swr <= 0 {
		rc.swr = rc.ttl
	}
	if rc.maxBodySize <= 0 {
		rc.maxBodySize = 16 << 20
	}
	if rc.waitRefresh <= 0 {
		rc.waitRefresh = 30 * time.Second
	}
	if rc.refreshConc <= 0 {
		rc.refreshConc = 64
	}
	if rc.refreshConc > 1<<16 {
		rc.refreshConc = 1 << 16
	}
	if rc.now == nil {
		rc.now = time.Now
	}
	if len(cfg.Methods) > 0 {
		rc.methods = make(map[string]struct{}, len(cfg.Methods))
		for _, m := range cfg.Methods {
			rc.methods[m] = struct{}{}
		}
	} else {
		rc.methods = map[string]struct{}{
			"GET":  {},
			"HEAD": {},
		}
	}
	for _, h := range cfg.KeyHeaders {
		rc.keyHeaders = append(rc.keyHeaders, []byte(strings.ToLower(h)))
	}
	vary, _ := parseVary(nil, []byte(strings.Join(cfg.Vary, ",")))
	rc.vary = vary
	return rc
}

// Stats is a snapshot of cache counters.
type Stats struct {
	Entries          int
	Evictions        int64
	Hits             int64
	Misses           int64
	StaleHits        int64
	Revalidated      int64
	RefreshOK        int64
	RefreshFailed    int64
	ActiveRefreshes  int64
	Bypassed         int64
}

// Cache is a mountable fasthttp response cache. It is safe for concurrent use.
type Cache struct {
	cfg     atomic.Pointer[runtimeConfig]
	cfgMu   sync.Mutex
	lastCfg Config
	enabled atomic.Bool

	store *lruStore

	flightMu sync.Mutex
	flights  map[string]*flight

	bgActive atomic.Int64
	wg       sync.WaitGroup

	next atomic.Pointer[fasthttp.RequestHandler]

	closeOnce sync.Once
	closeCh   chan struct{}
	closed    atomic.Bool

	hits         atomic.Int64
	misses       atomic.Int64
	staleHits    atomic.Int64
	revalidated  atomic.Int64
	refreshOK    atomic.Int64
	refreshFail  atomic.Int64
	bypassed     atomic.Int64
}

type flight struct {
	done chan struct{}
	err  error
}

// New creates a cache from the given configuration.
func New(cfg Config) *Cache {
	c := &Cache{
		store:   newLRUStore(cfg.MaxEntries),
		flights: make(map[string]*flight),
		closeCh: make(chan struct{}),
	}
	rc := normalizeConfig(cfg)
	c.cfg.Store(&rc)
	c.lastCfg = cfg
	c.enabled.Store(!cfg.Disabled)
	return c
}

// Handler wraps next with the response cache. Mount it as any other fasthttp
// middleware:
//
//	c := responsecache.New(responsecache.Config{TTL: time.Minute})
//	srv.Handler = c.Handler(myHandler)
//
// The cache keeps a reference to the wrapped handler for background
// refreshes; call Close when the server shuts down.
func (c *Cache) Handler(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	c.next.Store(&next)
	return c.handle
}

// SetEnabled enables or disables the cache atomically at runtime. Disabled
// requests pass straight through to the wrapped handler.
func (c *Cache) SetEnabled(enabled bool) {
	c.enabled.Store(enabled)
}

// Enabled reports whether the cache is enabled.
func (c *Cache) Enabled() bool {
	return c.enabled.Load()
}

// UpdateConfig atomically updates the runtime configuration. The mutator
// receives a copy of the current Config; the cache switches to the normalized
// result when the mutator returns. Capacity and refresh concurrency changes
// take effect immediately.
func (c *Cache) UpdateConfig(mut func(*Config)) {
	c.cfgMu.Lock()
	cfg := c.lastCfg
	mut(&cfg)
	rc := normalizeConfig(cfg)
	c.cfg.Store(&rc)
	c.lastCfg = cfg
	c.enabled.Store(!cfg.Disabled)
	c.store.setMaxEntries(cfg.MaxEntries)
	c.cfgMu.Unlock()
}

// Stats returns a snapshot of the cache counters.
func (c *Cache) Stats() Stats {
	return Stats{
		Entries:         c.store.len(),
		Evictions:       c.store.evictions(),
		Hits:            c.hits.Load(),
		Misses:          c.misses.Load(),
		StaleHits:       c.staleHits.Load(),
		Revalidated:     c.revalidated.Load(),
		RefreshOK:       c.refreshOK.Load(),
		RefreshFailed:   c.refreshFail.Load(),
		ActiveRefreshes: c.bgActive.Load(),
		Bypassed:        c.bypassed.Load(),
	}
}

// Close stops accepting cache operations and waits for background refreshes
// to finish. After Close the middleware passes every request straight through
// to the wrapped handler.
func (c *Cache) Close() error {
	return c.CloseWithContext(context.Background())
}

// CloseWithContext is like Close but bounds the wait for in-flight background
// refreshes with ctx.
func (c *Cache) CloseWithContext(ctx context.Context) error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		close(c.closeCh)
	})
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// InvalidateKey removes a single entry by its full key. It returns whether
// an entry was removed. Use BaseKey to compute keys for custom KeyFunc setups.
func (c *Cache) InvalidateKey(key string) bool {
	return c.store.del(key)
}

// InvalidateNamespace removes all entries tagged with the given namespace.
// It returns the number of removed entries.
func (c *Cache) InvalidateNamespace(namespace string) int {
	return c.store.delNamespaces([]string{namespace})
}

// InvalidateNamespaces removes all entries tagged with any of the given
// namespaces. It returns the number of removed entries.
func (c *Cache) InvalidateNamespaces(namespaces ...string) int {
	return c.store.delNamespaces(namespaces)
}

// InvalidateIf removes every entry for which pred returns true. It returns
// the number of removed entries. The predicate must be fast and must not
// block or call back into the cache.
func (c *Cache) InvalidateIf(pred func(EntryMeta) bool) int {
	return c.store.delIf(pred)
}

// InvalidateAll removes every entry. It returns the number of removed entries.
func (c *Cache) InvalidateAll() int {
	return c.store.clear()
}

// BaseKey computes the base cache key (without the Vary variant suffix) for a
// request, using the current configuration.
func (c *Cache) BaseKey(ctx *fasthttp.RequestCtx) string {
	return c.baseKey(c.cfg.Load(), ctx)
}

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

func (c *Cache) handle(ctx *fasthttp.RequestCtx) {
	if c.closed.Load() || !c.enabled.Load() {
		c.invokeNext(ctx)
		return
	}
	cfg := c.cfg.Load()

	if _, ok := cfg.methods[string(ctx.Method())]; !ok {
		c.invokeNext(ctx)
		if cfg.autoInvalid && ctx.Response.StatusCode() < 400 {
			if nss := namespacesOf(cfg, ctx); len(nss) > 0 {
				c.store.delNamespaces(nss)
			}
		}
		return
	}

	if cfg.cacheableReq != nil && !cfg.cacheableReq(ctx) {
		c.bypass(cfg, ctx)
		return
	}

	reqCC := parseRequestCacheControl(ctx.Request.Header.Peek(fasthttp.HeaderCacheControl))
	if pragma := ctx.Request.Header.Peek(fasthttp.HeaderPragma); bytes.Contains(bytes.ToLower(pragma), []byte("no-cache")) {
		reqCC.noCache = true
	}
	if reqCC.noStore {
		c.bypass(cfg, ctx)
		return
	}

	base := c.baseKey(cfg, ctx)
	if len(base) == 0 {
		c.bypass(cfg, ctx)
		return
	}

	now := cfg.now()
	if en := c.lookup(cfg, ctx, base); en != nil {
		fresh := en.fresh(now)
		if fresh && reqCC.noCache {
			fresh = false
		}
		if fresh && reqCC.maxAge == 0 {
			fresh = false
		}
		if fresh && reqCC.maxAge > 0 && now.Sub(en.storedAt) > time.Duration(reqCC.maxAge)*time.Second {
			fresh = false
		}
		if fresh && reqCC.hasMinFresh && now.Add(reqCC.minFresh).After(en.expiresAt) {
			fresh = false
		}
		if fresh {
			c.serveCached(cfg, ctx, en, now, xCacheHit)
			return
		}
		if en.allowsStale(now, reqCC) {
			c.serveCached(cfg, ctx, en, now, xCacheStale)
			c.startBackgroundRefresh(cfg, en)
			return
		}
		if reqCC.onlyIfCached {
			ctx.Response.Reset()
			ctx.SetStatusCode(fasthttp.StatusGatewayTimeout)
			ctx.SetBodyString("Gateway Timeout: only-if-cached and no fresh response available")
			return
		}
		c.fetchSynchronous(cfg, ctx, base, en)
		return
	}

	if reqCC.onlyIfCached {
		ctx.Response.Reset()
		ctx.SetStatusCode(fasthttp.StatusGatewayTimeout)
		ctx.SetBodyString("Gateway Timeout: only-if-cached and no cached response available")
		return
	}
	c.fetchSynchronous(cfg, ctx, base, nil)
}

func (c *Cache) invokeNext(ctx *fasthttp.RequestCtx) {
	if hp := c.next.Load(); hp != nil {
		(*hp)(ctx)
	}
}

func (c *Cache) bypass(cfg *runtimeConfig, ctx *fasthttp.RequestCtx) {
	c.invokeNext(ctx)
	c.bypassed.Add(1)
	c.setXCache(cfg, ctx, xCacheBypass)
}

// fetchSynchronous ensures the entry for base is (re)computated by at most
// one goroutine. Concurrent requests wait for the in-flight computation and
// serve the freshly stored entry; waiting interrupts on request cancellation,
// cache shutdown or the WaitForRefresh timeout, in which case the request
// falls back to invoking the handler directly.
func (c *Cache) fetchSynchronous(cfg *runtimeConfig, ctx *fasthttp.RequestCtx, base string, stale *entry) {
	// A background refresh may already be running for the exact variant.
	// Prefer waiting for it.
	if stale != nil {
		if f := c.peekFlight(stale.key); f != nil && c.waitFlight(cfg, ctx, f) {
			now := cfg.now()
			if en := c.lookup(cfg, ctx, base); en != nil && en.fresh(now) {
				c.serveCached(cfg, ctx, en, now, xCacheHit)
				return
			}
		}
	}

	f, leader := c.beginFlight(base)
	if leader {
		c.invokeNext(ctx)
		en := c.storeResponse(cfg, ctx, base)
		c.endFlight(base, nil)
		if en != nil {
			c.misses.Add(1)
			c.afterStoreServe(cfg, ctx, en)
		} else {
			c.bypassed.Add(1)
			c.setXCache(cfg, ctx, xCacheBypass)
		}
		return
	}

	if c.waitFlight(cfg, ctx, f) {
		now := cfg.now()
		if en := c.lookup(cfg, ctx, base); en != nil && (en.fresh(now) || en.allowsStale(now, requestDirectives{})) {
			c.serveCached(cfg, ctx, en, now, xCacheHit)
			return
		}
	}
	// Interrupted or no usable entry after refresh: compute directly.
	c.invokeNext(ctx)
	if en := c.storeResponse(cfg, ctx, base); en != nil {
		c.afterStoreServe(cfg, ctx, en)
	}
}

// waitFlight waits for a refresh to finish. It returns false when the wait
// was interrupted by request cancellation, cache shutdown or the configured
// timeout.
func (c *Cache) waitFlight(cfg *runtimeConfig, ctx *fasthttp.RequestCtx, f *flight) bool {
	timer := time.NewTimer(cfg.waitRefresh)
	defer timer.Stop()
	select {
	case <-f.done:
		return true
	case <-ctx.Done():
		return false
	case <-c.closeCh:
		return false
	case <-timer.C:
		return false
	}
}

// serveCached writes an isolated copy of the entry to ctx and handles
// conditional requests (304 Not Modified).
func (c *Cache) serveCached(cfg *runtimeConfig, ctx *fasthttp.RequestCtx, en *entry, now time.Time, status string) {
	en.resp.CopyTo(&ctx.Response)

	age := int64(now.Sub(en.storedAt).Seconds())
	if age < 0 {
		age = 0
	}
	ctx.Response.Header.Set(fasthttp.HeaderAge, strconv.FormatInt(age, 10))
	c.setXCache(cfg, ctx, status)

	if c.conditionalMatch(ctx, en) {
		c.writeNotModified(ctx)
	}

	if status == xCacheStale {
		c.staleHits.Add(1)
	} else {
		c.hits.Add(1)
	}
}

// afterStoreServe finalizes a response produced by the wrapped handler: marks
// it as a miss and converts it into a 304 when the request validators match.
func (c *Cache) afterStoreServe(cfg *runtimeConfig, ctx *fasthttp.RequestCtx, en *entry) {
	c.setXCache(cfg, ctx, xCacheMiss)
	if c.conditionalMatch(ctx, en) {
		c.writeNotModified(ctx)
	}
}

func (c *Cache) writeNotModified(ctx *fasthttp.RequestCtx) {
	ctx.Response.SetStatusCode(fasthttp.StatusNotModified)
	ctx.Response.ResetBody()
	ctx.Response.Header.Del(fasthttp.HeaderContentLength)
	ctx.Response.Header.Del(fasthttp.HeaderContentEncoding)
}

func (c *Cache) setXCache(cfg *runtimeConfig, ctx *fasthttp.RequestCtx, value string) {
	if cfg.addStatusHdr {
		ctx.Response.Header.Set("X-Cache", value)
	}
}

// conditionalMatch reports whether the request validators (If-None-Match /
// If-Modified-Since) match the stored entry.
func (c *Cache) conditionalMatch(ctx *fasthttp.RequestCtx, en *entry) bool {
	if inm := ctx.Request.Header.Peek(fasthttp.HeaderIfNoneMatch); len(inm) > 0 {
		return etagListMatch(inm, en.etag)
	}
	if ims := ctx.Request.Header.Peek(fasthttp.HeaderIfModifiedSince); len(ims) > 0 && en.hasLastMod {
		if t, err := fasthttp.ParseHTTPDate(ims); err == nil {
			return !t.Before(en.lastMod.Truncate(time.Second))
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Background refresh (stale-while-revalidate)
// ---------------------------------------------------------------------------

// startBackgroundRefresh triggers a background refresh for en unless one is
// already in flight for the same key (singleflight).
func (c *Cache) startBackgroundRefresh(cfg *runtimeConfig, en *entry) {
	if _, leader := c.beginFlight(en.key); !leader {
		return
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		if err := c.acquireRefreshSlot(cfg); err != nil {
			c.endFlight(en.key, err)
			c.refreshFail.Add(1)
			return
		}
		defer c.releaseRefreshSlot()
		err := c.executeRefresh(en)
		c.endFlight(en.key, err)
		if err != nil {
			c.refreshFail.Add(1)
		} else {
			c.refreshOK.Add(1)
		}
	}()
}

// acquireRefreshSlot bounds background refresh concurrency. It is responsive
// to cache shutdown while queued.
func (c *Cache) acquireRefreshSlot(cfg *runtimeConfig) error {
	for {
		if c.closed.Load() {
			return errClosed
		}
		cur := c.bgActive.Load()
		if cur < int64(cfg.refreshConc) && c.bgActive.CompareAndSwap(cur, cur+1) {
			return nil
		}
		select {
		case <-c.closeCh:
			return errClosed
		case <-time.After(time.Millisecond):
		}
	}
}

func (c *Cache) releaseRefreshSlot() {
	c.bgActive.Add(-1)
}

// executeRefresh recomputes the response for en using a synthetic request
// context. On a 304 the stored entry is revalidated in place; on a cacheable
// response the entry is replaced; on any failure the previous entry is kept.
func (c *Cache) executeRefresh(en *entry) (err error) {
	cfg := c.cfg.Load()
	hp := c.next.Load()
	if hp == nil {
		return errNoHandler
	}

	var req fasthttp.Request
	en.req.CopyTo(&req)
	if len(en.etag) > 0 {
		req.Header.Set(fasthttp.HeaderIfNoneMatch, string(en.etag))
	}
	if en.hasLastMod {
		req.Header.Set(fasthttp.HeaderIfModifiedSince, string(fasthttp.AppendHTTPDate(nil, en.lastMod)))
	}

	var rctx fasthttp.RequestCtx
	rctx.Init(&req, nil, cfg.logger)
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("responsecache: refresh handler panic: %v", r)
			}
		}()
		(*hp)(&rctx)
	}()
	if err != nil {
		c.logf(cfg, "%v", err)
		return err
	}
	if c.closed.Load() {
		return errClosed
	}

	switch rctx.Response.StatusCode() {
	case fasthttp.StatusNotModified:
		if c.revalidateEntry(cfg, en, &rctx) {
			c.revalidated.Add(1)
			return nil
		}
		return errNotStored
	default:
		newEn := c.buildEntry(cfg, &rctx, en.base)
		if newEn == nil {
			return errNotStored
		}
		c.store.put(en.base, newEn)
		if newEn.key != en.key {
			// Vary dimensions changed: drop the stale variant.
			c.store.del(en.key)
		}
		return nil
	}
}

// revalidateEntry produces a replacement entry from a 304 response, keeping
// the stored body and refreshing validators, caching headers and freshness
// windows.
func (c *Cache) revalidateEntry(cfg *runtimeConfig, en *entry, rctx *fasthttp.RequestCtx) bool {
	newEn := en.clone()
	h := &rctx.Response.Header

	if v := h.Peek(fasthttp.HeaderETag); len(v) > 0 {
		newEn.resp.Header.Set(fasthttp.HeaderETag, string(v))
		newEn.etag = append(newEn.etag[:0], v...)
	}
	if v := h.Peek(fasthttp.HeaderLastModified); len(v) > 0 {
		if t, err := fasthttp.ParseHTTPDate(v); err == nil {
			newEn.resp.Header.Set(fasthttp.HeaderLastModified, string(v))
			newEn.lastMod = t
			newEn.hasLastMod = true
		}
	}
	if v := h.Peek(fasthttp.HeaderCacheControl); len(v) > 0 {
		newEn.resp.Header.Set(fasthttp.HeaderCacheControl, string(v))
	}
	if v := h.Peek(fasthttp.HeaderExpires); len(v) > 0 {
		newEn.resp.Header.Set(fasthttp.HeaderExpires, string(v))
	}

	now := cfg.now()
	cc := parseResponseCacheControl(h.Peek(fasthttp.HeaderCacheControl))
	ttl := cfg.ttl
	switch {
	case cc.hasMaxAge:
		ttl = cc.maxAge
	case len(h.Peek(fasthttp.HeaderExpires)) > 0:
		if t, err := fasthttp.ParseHTTPDate(h.Peek(fasthttp.HeaderExpires)); err == nil {
			ttl = t.Sub(now)
		} else {
			ttl = 0
		}
	}
	if cc.noCache {
		ttl = 0
		newEn.noCache = true
	}
	if ttl < 0 {
		ttl = 0
	}
	swr := cfg.swr
	if cc.hasSWR {
		swr = cc.swr
	}
	newEn.mustReval = cc.mustRevalidate
	newEn.storedAt = now
	newEn.expiresAt = now.Add(ttl)
	newEn.staleUntil = newEn.expiresAt.Add(swr)
	if newEn.mustReval || newEn.noCache {
		newEn.staleUntil = newEn.expiresAt
	}
	c.store.put(en.base, newEn)
	return true
}

func (c *Cache) logf(cfg *runtimeConfig, format string, args ...any) {
	if cfg.logger != nil {
		cfg.logger.Printf(format, args...)
	}
}

// ---------------------------------------------------------------------------
// Key construction and storage
// ---------------------------------------------------------------------------

var keySep = []byte{0}

// baseKey builds the stable cache key from method, URI and caller selected
// request headers (or Config.KeyFunc when provided).
func (c *Cache) baseKey(cfg *runtimeConfig, ctx *fasthttp.RequestCtx) string {
	if cfg.keyFunc != nil {
		if k := cfg.keyFunc(ctx); len(k) > 0 {
			return "f:" + k
		}
		return ""
	}
	h := fnv.New64a()
	h.Write(ctx.Method())
	h.Write(keySep)
	h.Write(ctx.RequestURI())
	for _, name := range cfg.keyHeaders {
		h.Write(name)
		h.Write(keySep)
		h.Write(bytes.TrimSpace(ctx.Request.Header.PeekBytes(name)))
		h.Write(keySep)
	}
	return strconv.FormatUint(h.Sum64(), 16)
}

// variantKey extends the base key with the values of Vary request headers.
func variantKey(base string, ctx *fasthttp.RequestCtx, vary [][]byte) string {
	if len(vary) == 0 {
		return base
	}
	h := fnv.New64a()
	for _, name := range vary {
		h.Write(name)
		h.Write(keySep)
		h.Write(bytes.TrimSpace(ctx.Request.Header.PeekBytes(name)))
		h.Write(keySep)
	}
	return base + "|" + strconv.FormatUint(h.Sum64(), 16)
}

func (c *Cache) lookup(cfg *runtimeConfig, ctx *fasthttp.RequestCtx, base string) *entry {
	vary := mergeVary(cfg.vary, c.store.varyFor(base))
	return c.store.get(variantKey(base, ctx, vary))
}

// storeResponse snapshots the current response into the cache when it is
// cacheable and returns the stored entry.
func (c *Cache) storeResponse(cfg *runtimeConfig, ctx *fasthttp.RequestCtx, base string) *entry {
	en := c.buildEntry(cfg, ctx, base)
	if en == nil {
		return nil
	}
	c.store.put(base, en)
	return en
}

// buildEntry snapshots the response (and request) into an immutable entry.
// It returns nil when the response must not be cached.
func (c *Cache) buildEntry(cfg *runtimeConfig, ctx *fasthttp.RequestCtx, base string) *entry {
	if ctx.IsBodyStream() {
		return nil
	}
	status := ctx.Response.StatusCode()
	if cfg.cacheableResp != nil {
		if !cfg.cacheableResp(ctx, status) {
			return nil
		}
	} else if !defaultCacheableStatus(status) {
		return nil
	}
	cc := parseResponseCacheControl(ctx.Response.Header.Peek(fasthttp.HeaderCacheControl))
	if cc.noStore {
		return nil
	}
	body := ctx.Response.Body()
	if cfg.maxBodySize > 0 && len(body) > cfg.maxBodySize {
		return nil
	}
	vary, star := parseVary(cfg.vary, ctx.Response.Header.Peek(fasthttp.HeaderVary))
	if star {
		return nil
	}

	now := cfg.now()
	en := &entry{
		base:      base,
		storedAt:  now,
		noCache:   cc.noCache,
		mustReval: cc.mustRevalidate,
	}
	ctx.Response.CopyTo(&en.resp)
	ctx.Request.CopyTo(&en.req)
	en.vary = vary

	en.etag = append([]byte(nil), en.resp.Header.Peek(fasthttp.HeaderETag)...)
	if len(en.etag) == 0 && cfg.genETag && len(body) > 0 {
		h := fnv.New64a()
		h.Write(body)
		tag := fmt.Sprintf(`W/"%x"`, h.Sum64())
		en.etag = []byte(tag)
		en.resp.Header.Set(fasthttp.HeaderETag, tag)
		// Expose the generated validator to the current client as well.
		ctx.Response.Header.Set(fasthttp.HeaderETag, tag)
	}

	if lm := en.resp.Header.Peek(fasthttp.HeaderLastModified); len(lm) > 0 {
		if t, err := fasthttp.ParseHTTPDate(lm); err == nil {
			en.lastMod = t
			en.hasLastMod = true
		}
	}

	ttl := cfg.ttl
	switch {
	case cc.hasMaxAge:
		ttl = cc.maxAge
	case len(en.resp.Header.Peek(fasthttp.HeaderExpires)) > 0:
		if t, err := fasthttp.ParseHTTPDate(en.resp.Header.Peek(fasthttp.HeaderExpires)); err == nil {
			ttl = t.Sub(now)
		} else {
			ttl = 0 // invalid Expires means already expired
		}
	}
	if en.noCache {
		ttl = 0
	}
	if ttl < 0 {
		ttl = 0
	}
	en.expiresAt = now.Add(ttl)

	swr := cfg.swr
	if cc.hasSWR {
		swr = cc.swr
	}
	en.staleUntil = en.expiresAt.Add(swr)
	if en.mustReval || en.noCache {
		en.staleUntil = en.expiresAt
	}

	en.namespaces = namespacesOf(cfg, ctx)
	en.key = variantKey(base, ctx, en.vary)
	return en
}

func namespacesOf(cfg *runtimeConfig, ctx *fasthttp.RequestCtx) []string {
	if cfg.nsFunc == nil {
		return nil
	}
	return append([]string(nil), cfg.nsFunc(ctx)...)
}

func defaultCacheableStatus(status int) bool {
	switch status {
	case fasthttp.StatusOK,
		fasthttp.StatusNonAuthoritativeInfo,
		fasthttp.StatusMovedPermanently,
		fasthttp.StatusPermanentRedirect,
		fasthttp.StatusNotFound,
		fasthttp.StatusGone:
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// Singleflight
// ---------------------------------------------------------------------------

func (c *Cache) beginFlight(key string) (*flight, bool) {
	c.flightMu.Lock()
	defer c.flightMu.Unlock()
	if f, ok := c.flights[key]; ok {
		return f, false
	}
	f := &flight{done: make(chan struct{})}
	c.flights[key] = f
	return f, true
}

func (c *Cache) peekFlight(key string) *flight {
	c.flightMu.Lock()
	f := c.flights[key]
	c.flightMu.Unlock()
	return f
}

func (c *Cache) endFlight(key string, err error) {
	c.flightMu.Lock()
	f := c.flights[key]
	delete(c.flights, key)
	c.flightMu.Unlock()
	if f != nil {
		f.err = err
		close(f.done)
	}
}
