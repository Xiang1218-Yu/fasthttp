package responsecache

import (
	"bytes"
	"sort"
	"strconv"
	"time"
)

// maxStaleForever is used when a request sends "Cache-Control: max-stale"
// without a delta-seconds value, meaning the client accepts stale responses
// of any age.
const maxStaleForever = 100 * 365 * 24 * time.Hour

// responseDirectives holds the parsed Cache-Control directives of a response
// that are relevant for response caching.
type responseDirectives struct {
	noStore        bool
	noCache        bool // response may be stored but must be revalidated before use
	mustRevalidate bool // stale responses must never be served
	maxAge         time.Duration
	hasMaxAge      bool
	swr            time.Duration
	hasSWR         bool
}

func parseResponseCacheControl(v []byte) responseDirectives {
	d := responseDirectives{maxAge: -1, swr: -1}
	var sMaxAge time.Duration
	hasSMaxAge := false
	forEachCacheDirective(v, func(name, val []byte) {
		switch string(name) {
		case "no-store":
			d.noStore = true
		case "no-cache":
			d.noCache = true
		case "must-revalidate", "proxy-revalidate":
			d.mustRevalidate = true
		case "max-age":
			if n, ok := parseDeltaSeconds(val); ok {
				d.maxAge = n
				d.hasMaxAge = true
			}
		case "s-maxage":
			if n, ok := parseDeltaSeconds(val); ok {
				sMaxAge = n
				hasSMaxAge = true
			}
		case "stale-while-revalidate":
			if n, ok := parseDeltaSeconds(val); ok {
				d.swr = n
				d.hasSWR = true
			}
		}
	})
	if hasSMaxAge {
		// s-maxage takes precedence over max-age for shared caches.
		d.maxAge = sMaxAge
		d.hasMaxAge = true
	}
	return d
}

// requestDirectives holds the parsed Cache-Control directives of a request
// that are relevant for response caching.
type requestDirectives struct {
	noStore       bool
	noCache       bool
	onlyIfCached  bool
	maxAge        int // -1 means unset; acceptable maximum age in seconds
	maxStale      time.Duration
	hasMaxStale   bool
	minFresh      time.Duration
	hasMinFresh   bool
}

func parseRequestCacheControl(v []byte) requestDirectives {
	d := requestDirectives{maxAge: -1}
	forEachCacheDirective(v, func(name, val []byte) {
		switch string(name) {
		case "no-store":
			d.noStore = true
		case "no-cache":
			d.noCache = true
		case "only-if-cached":
			d.onlyIfCached = true
		case "max-age":
			if n, ok := parseDeltaSeconds(val); ok {
				d.maxAge = int(n / time.Second)
			}
		case "max-stale":
			d.hasMaxStale = true
			if len(val) == 0 {
				d.maxStale = maxStaleForever
			} else if n, ok := parseDeltaSeconds(val); ok {
				d.maxStale = n
			}
		case "min-fresh":
			if n, ok := parseDeltaSeconds(val); ok {
				d.minFresh = n
				d.hasMinFresh = true
			}
		}
	})
	return d
}

// forEachCacheDirective iterates over comma separated cache directives.
func forEachCacheDirective(v []byte, f func(name, val []byte)) {
	for len(v) > 0 {
		var part []byte
		if i := bytes.IndexByte(v, ','); i >= 0 {
			part, v = v[:i], v[i+1:]
		} else {
			part, v = v, nil
		}
		part = bytes.TrimSpace(part)
		if len(part) == 0 {
			continue
		}
		name, val := part, []byte(nil)
		if i := bytes.IndexByte(part, '='); i >= 0 {
			name = bytes.TrimSpace(part[:i])
			val = bytes.TrimSpace(part[i+1:])
			val = bytes.Trim(val, `"`)
		}
		f(bytes.ToLower(name), val)
	}
}

func parseDeltaSeconds(v []byte) (time.Duration, bool) {
	n, err := strconv.Atoi(string(v))
	if err != nil || n < 0 {
		return 0, false
	}
	return time.Duration(n) * time.Second, true
}

// parseVary normalizes the configured vary headers and the value of a Vary
// response header into a sorted, deduplicated, lower-cased list. It reports
// star=true when the response carries "Vary: *", in which case the response
// must not be cached.
func parseVary(configured [][]byte, v []byte) (names [][]byte, star bool) {
	seen := make(map[string]struct{})
	add := func(name []byte) {
		name = bytes.ToLower(bytes.TrimSpace(name))
		if len(name) == 0 {
			return
		}
		if string(name) == "*" {
			star = true
			return
		}
		if _, ok := seen[string(name)]; ok {
			return
		}
		seen[string(name)] = struct{}{}
		names = append(names, name)
	}
	for _, name := range configured {
		add(name)
	}
	for _, name := range bytes.Split(v, []byte(",")) {
		add(name)
	}
	if star {
		return nil, true
	}
	sort.Slice(names, func(i, j int) bool { return string(names[i]) < string(names[j]) })
	return names, false
}

// mergeVary merges two normalized vary header lists.
func mergeVary(a, b [][]byte) [][]byte {
	merged := make([][]byte, 0, len(a)+len(b))
	merged = append(merged, a...)
	merged = append(merged, b...)
	out := make([][]byte, 0, len(merged))
	seen := make(map[string]struct{}, len(merged))
	for _, n := range merged {
		if _, ok := seen[string(n)]; ok {
			continue
		}
		seen[string(n)] = struct{}{}
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i]) < string(out[j]) })
	return out
}

// etagListMatch reports whether any entity tag in an If-None-Match (or
// If-Match) header value matches the given etag. Comparison uses the weak
// comparison function (RFC 7232): the "W/" prefix is ignored on both sides
// and "*" matches anything.
func etagListMatch(headerValue, etag []byte) bool {
	if len(etag) == 0 {
		return false
	}
	rest := headerValue
	for len(rest) > 0 {
		var tok []byte
		if i := bytes.IndexByte(rest, ','); i >= 0 {
			tok, rest = rest[:i], rest[i+1:]
		} else {
			tok, rest = rest, nil
		}
		tok = bytes.TrimSpace(tok)
		if len(tok) == 0 {
			continue
		}
		if bytes.Equal(tok, []byte("*")) || weakETagEqual(tok, etag) {
			return true
		}
	}
	return false
}

func weakETagEqual(a, b []byte) bool {
	a = bytes.TrimPrefix(bytes.TrimSpace(a), []byte("W/"))
	b = bytes.TrimPrefix(bytes.TrimSpace(b), []byte("W/"))
	return bytes.Equal(a, b)
}
