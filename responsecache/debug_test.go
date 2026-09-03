package responsecache

import (
	"testing"
	"time"

	"github.com/valyala/fasthttp"
)

func TestDebugLM(t *testing.T) {
	lastMod := time.Unix(1700000000, 0).UTC()
	t.Logf("lastMod formatted: %q", lastMod.Format(time.RFC1123))
	parsed, err := fasthttp.ParseHTTPDate([]byte(lastMod.Format(time.RFC1123)))
	t.Logf("parsed=%v err=%v equal=%v", parsed, err, parsed.Equal(lastMod))

	c := newTestCache(t, Config{TTL: time.Minute})
	h := c.Handler(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("payload")
		ctx.Response.Header.Set(fasthttp.HeaderLastModified, lastMod.Format(time.RFC1123))
	})
	r1 := get(h, "/lm")
	t.Logf("miss: status=%d LM=%q ETag=%q body=%q", r1.Response.StatusCode(),
		r1.Response.Header.Peek(fasthttp.HeaderLastModified),
		r1.Response.Header.Peek(fasthttp.HeaderETag), r1.Response.Body())

	en := c.store.get(c.BaseKey(newCtx("GET", "/lm")))
	if en == nil {
		t.Fatal("no entry stored")
	}
	t.Logf("entry: hasLastMod=%v lastMod=%v lmPeek=%q", en.hasLastMod, en.lastMod, en.resp.Header.Peek(fasthttp.HeaderLastModified))

	ctx := newCtx("GET", "/lm")
	ctx.Request.Header.Set(fasthttp.HeaderIfModifiedSince, lastMod.Format(time.RFC1123))
	t.Logf("req IMS=%q", ctx.Request.Header.Peek(fasthttp.HeaderIfModifiedSince))
	h(ctx)
	t.Logf("status=%d body=%q", ctx.Response.StatusCode(), ctx.Response.Body())
}
