package fasthttp

import (
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"
)

// CookieJar manages storage and use of cookies in HTTP requests.
//
// Implementations must be safe for concurrent use by multiple goroutines.
// Assign a jar to Client.Jar or HostClient.Jar to have it automatically
// supply matching cookies on requests and absorb Set-Cookie headers from
// responses, including on redirect hops.
type CookieJar interface {
	// SetCookies stores the cookies received in a response's Set-Cookie
	// headers for the given request URL u.
	//
	// Expired cookies (Max-Age <= 0, Expires in the past) remove previously
	// stored entries with the same name, domain and path. The Domain, Path,
	// Expires/Max-Age, Secure and other attributes of each cookie follow
	// the rules of RFC 6265 section 5.3.
	SetCookies(u *URI, cookies []*Cookie)

	// Cookies returns the cookies to send in a request for the given URL u.
	//
	// Cookies are selected by domain (host-only or parent domain), path and
	// the Secure attribute (Secure cookies are only returned for https URLs),
	// expired cookies are skipped, and the result is ordered per RFC 6265
	// section 5.4: longer cookie paths first, earlier creation time first.
	Cookies(u *URI) []*Cookie
}

// NewCookieJar returns a new thread-safe in-memory CookieJar.
//
// The jar is safe for concurrent use and may be shared between clients and
// goroutines. Its contents can be persisted with (*cookieJar).Dump and
// restored with (*cookieJar).Load.
func NewCookieJar() *cookieJar {
	return &cookieJar{
		entries: make(map[string]map[string]*cookieJarEntry),
	}
}

// cookieJarEntry is a single stored cookie.
//
// Entries are grouped by their canonical (lower-cased, port-free) cookie
// domain: the response host for host-only cookies, or the Domain attribute
// value otherwise. Within a domain bucket the entry key is name + "\x00" +
// path, matching the (name, domain, path) cookie identity of RFC 6265.
type cookieJarEntry struct {
	Name        string         `json:"name"`
	Value       string         `json:"value"`
	Domain      string         `json:"domain"`
	Path        string         `json:"path"`
	Secure      bool           `json:"secure,omitempty"`
	HTTPOnly    bool           `json:"http_only,omitempty"`
	SameSite    CookieSameSite `json:"same_site,omitempty"`
	Partitioned bool           `json:"partitioned,omitempty"`
	HostOnly    bool           `json:"host_only,omitempty"`
	Persistent  bool           `json:"persistent,omitempty"`
	Expires     time.Time      `json:"expires,omitempty"`
	Creation    time.Time      `json:"creation"`

	// seq is the jar-wide insertion order used for the stable send order
	// (earlier created cookies first within one path length).
	seq uint64
}

type cookieJar struct {
	mu sync.RWMutex

	// entries maps a canonical cookie domain to a bucket of entries keyed
	// by name + "\x00" + path.
	entries map[string]map[string]*cookieJarEntry

	// seq is bumped for every newly created entry.
	seq uint64
}

// SetCookies implements CookieJar.
func (j *cookieJar) SetCookies(u *URI, cookies []*Cookie) {
	if u == nil || len(cookies) == 0 {
		return
	}

	// Copy every value retained in the jar: Cookie fields and URI fields
	// are backed by reusable/pooled byte buffers, and b2s strings alias
	// those buffers (a redirect reuses the request URI, AcquireCookie
	// buffers are Reset on release).
	host := jarCanonicalHost(u.Host())
	if host == "" {
		return
	}

	requestPath := string(u.Path())
	now := time.Now()

	j.mu.Lock()
	defer j.mu.Unlock()

	for _, cookie := range cookies {
		if cookie == nil || len(cookie.Key()) == 0 {
			continue
		}

		name := string(cookie.Key())

		domain := host
		hostOnly := true
		if rawDomain := string(cookie.Domain()); len(rawDomain) > 0 {
			canonicalDomain := strings.ToLower(strings.TrimPrefix(rawDomain, "."))
			// Reject cookies that claim a domain the response host is not
			// part of (RFC 6265 5.3 step 5), and bare single-label domains
			// such as "com", which a public-suffix list would normally
			// reject and which would otherwise leak cookies to unrelated
			// hosts sharing the suffix.
			if canonicalDomain == "" || !jarDomainMatch(host, canonicalDomain) ||
				!strings.Contains(canonicalDomain, ".") {
				continue
			}
			domain = canonicalDomain
			hostOnly = false
		}

		path := string(cookie.Path())
		if path == "" || path[0] != '/' {
			path = jarDefaultPath(requestPath)
		}

		expires, persistent, remove := jarCookieExpiry(cookie, now)
		if remove {
			j.deleteEntry(domain, name, path)
			continue
		}

		bucket := j.entries[domain]
		if bucket == nil {
			bucket = make(map[string]*cookieJarEntry)
			j.entries[domain] = bucket
		}

		key := jarEntryKey(name, path)
		entry := bucket[key]
		if entry == nil {
			j.seq++
			entry = &cookieJarEntry{
				Name:     name,
				Domain:   domain,
				Path:     path,
				HostOnly: hostOnly,
				Creation: now,
				seq:      j.seq,
			}
			bucket[key] = entry
		}

		entry.Value = string(cookie.Value())
		entry.Secure = cookie.Secure()
		entry.HTTPOnly = cookie.HTTPOnly()
		entry.SameSite = cookie.SameSite()
		entry.Partitioned = cookie.Partitioned()
		entry.HostOnly = hostOnly
		entry.Persistent = persistent
		entry.Expires = expires
	}
}

// Cookies implements CookieJar.
func (j *cookieJar) Cookies(u *URI) []*Cookie {
	if u == nil {
		return nil
	}

	host := jarCanonicalHost(u.Host())
	if host == "" {
		return nil
	}

	requestPath := b2s(u.Path())
	isTLS := u.isHTTPS()
	now := time.Now()

	j.mu.RLock()
	defer j.mu.RUnlock()

	var matched []*cookieJarEntry

	for _, domain := range jarDomainSuffixes(host) {
		hostOnlyDomain := domain == host
		bucket := j.entries[domain]
		for _, entry := range bucket {
			if entry.HostOnly && !hostOnlyDomain {
				continue
			}
			if entry.Secure && !isTLS {
				continue
			}
			if entry.Persistent && !entry.Expires.After(now) {
				continue
			}
			if entry.Path == "" || entry.Path[0] != '/' || !jarPathMatch(requestPath, entry.Path) {
				continue
			}
			matched = append(matched, entry)
		}
	}

	if len(matched) == 0 {
		return nil
	}

	sort.SliceStable(matched, func(i, k int) bool {
		if len(matched[i].Path) != len(matched[k].Path) {
			// Longer cookie paths come first.
			return len(matched[i].Path) > len(matched[k].Path)
		}
		// Earlier created cookies come first.
		return matched[i].seq < matched[k].seq
	})

	// Build detached Cookie copies while still holding the read lock:
	// returned objects must outlive the lock without aliasing entries.
	cookies := make([]*Cookie, 0, len(matched))
	for _, entry := range matched {
		cookie := &Cookie{}
		cookie.SetKey(entry.Name)
		cookie.SetValue(entry.Value)
		cookie.SetHTTPOnly(entry.HTTPOnly)
		cookie.SetSameSite(entry.SameSite)
		if entry.Partitioned {
			cookie.SetPartitioned(true)
		}
		cookie.SetSecure(entry.Secure)
		cookie.SetDomain(entry.Domain)
		cookie.SetPath(entry.Path)
		if entry.Persistent {
			cookie.SetExpire(entry.Expires)
		}
		cookies = append(cookies, cookie)
	}

	return cookies
}

// Len returns the number of cookies currently stored in the jar.
func (j *cookieJar) Len() int {
	j.mu.RLock()
	n := 0
	for _, bucket := range j.entries {
		n += len(bucket)
	}
	j.mu.RUnlock()
	return n
}

// Reset removes all cookies from the jar.
func (j *cookieJar) Reset() {
	j.mu.Lock()
	j.entries = make(map[string]map[string]*cookieJarEntry)
	j.mu.Unlock()
}

// cookieJarData is the JSON persistence format used by cookieJar.Dump and
// cookieJar.Load.
type cookieJarData struct {
	Entries []cookieJarEntry `json:"entries"`
}

// Dump exports all jar entries as JSON.
//
// Session cookies (cookies without an Expires or Max-Age attribute) are
// exported as well; their zero expiry marks them as session cookies again
// after Load.
func (j *cookieJar) Dump() ([]byte, error) {
	j.mu.RLock()
	data := cookieJarData{}
	for _, bucket := range j.entries {
		for _, entry := range bucket {
			data.Entries = append(data.Entries, *entry)
		}
	}
	j.mu.RUnlock()

	sort.Slice(data.Entries, func(i, k int) bool {
		a, b := data.Entries[i], data.Entries[k]
		if a.Domain != b.Domain {
			return a.Domain < b.Domain
		}
		if a.Path != b.Path {
			return a.Path < b.Path
		}
		return a.Name < b.Name
	})

	return json.Marshal(&data)
}

// Load imports entries previously produced with Dump, replacing all
// contents of the jar. Malformed data is rejected and leaves the jar
// untouched.
func (j *cookieJar) Load(data []byte) error {
	var parsed cookieJarData
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}

	entries := make(map[string]map[string]*cookieJarEntry)
	var seq uint64
	for i := range parsed.Entries {
		entry := parsed.Entries[i]
		if entry.Name == "" || entry.Domain == "" || entry.Path == "" || entry.Path[0] != '/' {
			continue
		}
		bucket := entries[entry.Domain]
		if bucket == nil {
			bucket = make(map[string]*cookieJarEntry)
			entries[entry.Domain] = bucket
		}
		seq++
		entry.seq = seq
		entryCopy := entry
		bucket[jarEntryKey(entry.Name, entry.Path)] = &entryCopy
	}

	j.mu.Lock()
	j.entries = entries
	j.seq = seq
	j.mu.Unlock()

	return nil
}

func (j *cookieJar) deleteEntry(domain, name, path string) {
	bucket := j.entries[domain]
	if bucket == nil {
		return
	}
	delete(bucket, jarEntryKey(name, path))
	if len(bucket) == 0 {
		delete(j.entries, domain)
	}
}

// jarCanonicalHost lower-cases the URL host and strips port, user info and
// IPv6 brackets for cookie domain comparisons. The result is an owned
// string, not aliasing the URI byte buffer.
func jarCanonicalHost(host []byte) string {
	return strings.ToLower(string(hostnameFromHostPortBytes(host)))
}

// jarDomainMatch reports whether host is identical to domain or a
// sub-domain of domain.
func jarDomainMatch(host, domain string) bool {
	if host == domain {
		return true
	}
	return strings.HasSuffix(host, "."+domain)
}

// jarDomainSuffixes returns the host itself followed by every parent domain
// suffix, i.e. the cookie domains that may match the host. IP literals are
// only matched against themselves.
func jarDomainSuffixes(host string) []string {
	if strings.IndexByte(host, ':') >= 0 {
		return []string{host}
	}

	suffixes := []string{host}
	for i := 0; i < len(host); i++ {
		if host[i] == '.' && i+1 < len(host) {
			suffixes = append(suffixes, host[i+1:])
		}
	}
	return suffixes
}

// jarPathMatch implements the cookie path matching rule of RFC 6265
// section 5.1.4. Both paths are expected to start with '/'.
func jarPathMatch(requestPath, cookiePath string) bool {
	if requestPath == cookiePath {
		return true
	}
	if !strings.HasPrefix(requestPath, cookiePath) {
		return false
	}
	if cookiePath[len(cookiePath)-1] == '/' {
		return true
	}
	return requestPath[len(cookiePath)] == '/'
}

// jarDefaultPath computes the default cookie path of RFC 6265 section
// 5.1.4 from the request URL path.
func jarDefaultPath(uriPath string) string {
	if uriPath == "" || uriPath[0] != '/' {
		return "/"
	}
	// Look for the right-most slash excluding the leading one.
	if i := strings.LastIndexByte(uriPath[1:], '/'); i >= 0 {
		return uriPath[:i+1]
	}
	return "/"
}

// jarCookieExpiry resolves the effective expiry of a Set-Cookie value.
// Max-Age takes precedence over Expires. The returned remove flag signals
// that the cookie must be deleted (Max-Age <= 0 or Expires in the past).
func jarCookieExpiry(cookie *Cookie, now time.Time) (expires time.Time, persistent, remove bool) {
	switch maxAge := cookie.MaxAge(); {
	case maxAge < 0:
		return time.Time{}, false, true
	case maxAge > 0:
		return now.Add(time.Duration(maxAge) * time.Second), true, false
	}

	expire := cookie.Expire()
	if expire.IsZero() {
		// Session cookie.
		return time.Time{}, false, false
	}
	if !expire.After(now) {
		return time.Time{}, true, true
	}
	return expire, true, false
}

func jarEntryKey(name, path string) string {
	return name + "\x00" + path
}
