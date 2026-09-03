package responsecache

import (
	"container/list"
	"sync"
	"time"

	"github.com/valyala/fasthttp"
)

// EntryMeta is a read-only snapshot of a cached entry exposed to custom
// invalidation predicates.
type EntryMeta struct {
	// Key is the full cache key (base key plus the Vary variant suffix).
	Key string
	// StatusCode is the stored HTTP status code.
	StatusCode int
	// Namespaces are the tags attached via Config.NamespaceFunc.
	Namespaces []string
	// Vary is the sorted list of normalized request header names the
	// entry varies on.
	Vary []string
	// ETag is the entity tag of the stored response.
	ETag []byte
	// StoredAt is when the entry was stored or last successfully revalidated.
	StoredAt time.Time
	// ExpiresAt is when the entry becomes stale.
	ExpiresAt time.Time
	// StaleUntil is the end of the stale-while-revalidate window.
	StaleUntil time.Time
}

// entry is an immutable cached response. Entries are never mutated after
// publication; refresh and revalidation replace them atomically in the store,
// which guarantees that concurrent readers copying a snapshot never race.
type entry struct {
	base string // base key (method + URI + caller selected headers)
	key  string // full key including the Vary variant suffix

	resp fasthttp.Response // owned snapshot; read-only after publication
	req  fasthttp.Request  // request template for background revalidation

	vary       [][]byte // sorted, lower-cased header names
	namespaces []string

	etag       []byte
	lastMod    time.Time
	hasLastMod bool

	storedAt   time.Time
	expiresAt  time.Time
	staleUntil time.Time

	noCache   bool // stored "no-cache": must revalidate before every serve
	mustReval bool // "must-revalidate"/"proxy-revalidate": never serve stale

	elem *list.Element // LRU list element
}

func (e *entry) fresh(now time.Time) bool {
	return now.Before(e.expiresAt)
}

// allowsStale reports whether the entry may be served as stale considering
// both its own freshness window and the request's cache directives.
func (e *entry) allowsStale(now time.Time, d requestDirectives) bool {
	if e.noCache || e.mustReval {
		return false
	}
	if d.noCache || d.maxAge == 0 {
		return false
	}
	staleEnd := e.staleUntil
	if d.hasMaxStale {
		if end := e.expiresAt.Add(d.maxStale); end.After(staleEnd) {
			staleEnd = end
		}
	}
	return now.Before(staleEnd)
}

func (e *entry) meta() EntryMeta {
	vary := make([]string, len(e.vary))
	for i, n := range e.vary {
		vary[i] = string(n)
	}
	return EntryMeta{
		Key:        e.key,
		StatusCode: e.resp.StatusCode(),
		Namespaces: append([]string(nil), e.namespaces...),
		Vary:       vary,
		ETag:       append([]byte(nil), e.etag...),
		StoredAt:   e.storedAt,
		ExpiresAt:  e.expiresAt,
		StaleUntil: e.staleUntil,
	}
}

// clone returns a deep, mutable copy of an immutable entry. Used by 304
// revalidation to produce a replacement entry.
func (e *entry) clone() *entry {
	n := &entry{
		base:       e.base,
		key:        e.key,
		vary:       append([][]byte(nil), e.vary...),
		namespaces: append([]string(nil), e.namespaces...),
		etag:       append([]byte(nil), e.etag...),
		lastMod:    e.lastMod,
		hasLastMod: e.hasLastMod,
		storedAt:   e.storedAt,
		expiresAt:  e.expiresAt,
		staleUntil: e.staleUntil,
		noCache:    e.noCache,
		mustReval:  e.mustReval,
	}
	e.resp.CopyTo(&n.resp)
	e.req.CopyTo(&n.req)
	return n
}

// lruStore is a capacity bounded LRU store with Vary and namespace indexes.
type lruStore struct {
	mu       sync.Mutex
	items    map[string]*list.Element
	lru      *list.List
	maxItems int

	varyIdx map[string][][]byte            // base key -> vary header names
	nsIdx   map[string]map[string]struct{} // namespace -> set of full keys

	evicted int64
}

func newLRUStore(maxItems int) *lruStore {
	return &lruStore{
		items:    make(map[string]*list.Element),
		lru:      list.New(),
		maxItems: maxItems,
		varyIdx:  make(map[string][][]byte),
		nsIdx:    make(map[string]map[string]struct{}),
	}
}

func (s *lruStore) get(key string) *entry {
	s.mu.Lock()
	el, ok := s.items[key]
	if !ok {
		s.mu.Unlock()
		return nil
	}
	s.lru.MoveToFront(el)
	e := el.Value.(*entry)
	s.mu.Unlock()
	return e
}

func (s *lruStore) varyFor(baseKey string) [][]byte {
	s.mu.Lock()
	v := s.varyIdx[baseKey]
	out := append([][]byte(nil), v...)
	s.mu.Unlock()
	return out
}

// put stores e under key, replacing any previous entry for the same key and
// evicting least recently used entries when over capacity.
func (s *lruStore) put(baseKey string, e *entry) {
	s.mu.Lock()
	if old, ok := s.items[e.key]; ok {
		s.removeLocked(old)
	}
	e.elem = s.lru.PushFront(e)
	s.items[e.key] = e.elem
	s.varyIdx[baseKey] = append([][]byte(nil), e.vary...)
	for _, ns := range e.namespaces {
		set, ok := s.nsIdx[ns]
		if !ok {
			set = make(map[string]struct{})
			s.nsIdx[ns] = set
		}
		set[e.key] = struct{}{}
	}
	for s.maxItems > 0 && s.lru.Len() > s.maxItems {
		s.removeLocked(s.lru.Back())
		s.evicted++
	}
	s.mu.Unlock()
}

func (s *lruStore) removeLocked(el *list.Element) {
	if el == nil {
		return
	}
	e := el.Value.(*entry)
	s.lru.Remove(el)
	delete(s.items, e.key)
	for _, ns := range e.namespaces {
		if set, ok := s.nsIdx[ns]; ok {
			delete(set, e.key)
			if len(set) == 0 {
				delete(s.nsIdx, ns)
			}
		}
	}
}

func (s *lruStore) del(key string) bool {
	s.mu.Lock()
	el, ok := s.items[key]
	if ok {
		s.removeLocked(el)
	}
	s.mu.Unlock()
	return ok
}

func (s *lruStore) delNamespaces(namespaces []string) int {
	s.mu.Lock()
	seen := make(map[string]struct{})
	var keys []string
	for _, ns := range namespaces {
		for k := range s.nsIdx[ns] {
			if _, ok := seen[k]; !ok {
				seen[k] = struct{}{}
				keys = append(keys, k)
			}
		}
	}
	for _, k := range keys {
		if el, ok := s.items[k]; ok {
			s.removeLocked(el)
		}
	}
	s.mu.Unlock()
	return len(keys)
}

func (s *lruStore) delIf(pred func(EntryMeta) bool) int {
	s.mu.Lock()
	var keys []string
	for _, el := range s.items {
		e := el.Value.(*entry)
		if pred(e.meta()) {
			keys = append(keys, e.key)
		}
	}
	for _, k := range keys {
		if el, ok := s.items[k]; ok {
			s.removeLocked(el)
		}
	}
	s.mu.Unlock()
	return len(keys)
}

func (s *lruStore) clear() int {
	s.mu.Lock()
	n := s.lru.Len()
	s.items = make(map[string]*list.Element)
	s.lru.Init()
	s.varyIdx = make(map[string][][]byte)
	s.nsIdx = make(map[string]map[string]struct{})
	s.mu.Unlock()
	return n
}

func (s *lruStore) len() int {
	s.mu.Lock()
	n := s.lru.Len()
	s.mu.Unlock()
	return n
}

func (s *lruStore) evictions() int64 {
	s.mu.Lock()
	n := s.evicted
	s.mu.Unlock()
	return n
}

// setMaxEntries updates the capacity at runtime and evicts entries when the
// new capacity is smaller than the current size.
func (s *lruStore) setMaxEntries(maxItems int) {
	s.mu.Lock()
	s.maxItems = maxItems
	for s.maxItems > 0 && s.lru.Len() > s.maxItems {
		s.removeLocked(s.lru.Back())
		s.evicted++
	}
	s.mu.Unlock()
}
