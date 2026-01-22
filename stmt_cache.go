package zorm

import (
	"container/list"
	"database/sql"
	"sync"
	"sync/atomic"
)

// StmtCache provides a thread-safe LRU cache for prepared statements.
// It stores prepared SQL statements and automatically evicts the least
// recently used entries when the cache reaches its maximum capacity.
//
// The cache is safe for concurrent use by multiple goroutines and helps
// improve performance by reusing prepared statements instead of re-preparing
// them on every execution.
type StmtCache struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*cacheEntry
	lruList  *list.List
}

// cacheEntry represents a cached prepared statement with its LRU tracking element.
type cacheEntry struct {
	stmt     *sql.Stmt
	element  *list.Element
	refCount int32
	evicted  bool
	query    string
}

// NewStmtCache creates a new statement cache with the specified capacity.
// When the cache reaches capacity, the least recently used statement will
// be evicted to make room for new entries.
//
// A capacity of 0 or negative value will default to 100.
func NewStmtCache(capacity int) *StmtCache {
	if capacity <= 0 {
		capacity = 100
	}
	return &StmtCache{
		capacity: capacity,
		items:    make(map[string]*cacheEntry),
		lruList:  list.New(),
	}
}

// Get retrieves a cached prepared statement for the given SQL query.
// Returns the statement and a release function. The caller MUST call the release function
// when finished using the statement.
// Returns nil, nil if the statement is not found in the cache.
func (c *StmtCache) Get(query string) (*sql.Stmt, func()) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, exists := c.items[query]; exists {
		c.lruList.MoveToFront(entry.element)
		atomic.AddInt32(&entry.refCount, 1)
		return entry.stmt, func() {
			c.release(entry)
		}
	}

	return nil, nil
}

// Put stores a prepared statement in the cache for the given SQL query.
// If the cache is at capacity, the least recently used statement will be
// evicted (and closed when no longer in use) before adding the new statement.
//
// If a statement with the same query already exists, it will be replaced.
func (c *StmtCache) Put(query string, stmt *sql.Stmt) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.putLocked(query, stmt)
}

// putLocked is the internal implementation of Put, assumes lock is held.
func (c *StmtCache) putLocked(query string, stmt *sql.Stmt) *cacheEntry {
	// If entry already exists, update it?
	// It's tricky to update an existing entry safely if it's in use.
	// We should evict the old one and add new one.
	if entry, exists := c.items[query]; exists {
		c.evictEntry(entry)
	}

	// Evict LRU entry if at capacity
	if len(c.items) >= c.capacity {
		c.evictLRU()
	}

	entry := &cacheEntry{
		stmt:     stmt,
		query:    query,
		refCount: 0,
		evicted:  false,
	}
	element := c.lruList.PushFront(entry)
	entry.element = element
	c.items[query] = entry
	return entry
}

// PutAndGet atomically stores a prepared statement and retrieves it with
// an incremented reference count. This avoids race conditions where the
// statement could be evicted between Put and Get calls.
// Returns the statement and a release function. The caller MUST call the release function.
func (c *StmtCache) PutAndGet(query string, stmt *sql.Stmt) (*sql.Stmt, func()) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry := c.putLocked(query, stmt)
	atomic.AddInt32(&entry.refCount, 1)
	return entry.stmt, func() {
		c.release(entry)
	}
}

// evictLRU removes the least recently used statement from the cache.
func (c *StmtCache) evictLRU() {
	element := c.lruList.Back()
	if element == nil {
		return
	}
	entry := element.Value.(*cacheEntry)
	c.evictEntry(entry)
}

// evictEntry removes an entry from the map and list, marking it as evicted.
func (c *StmtCache) evictEntry(entry *cacheEntry) {
	c.lruList.Remove(entry.element)
	delete(c.items, entry.query)
	entry.evicted = true

	// If no one is using it, close immediately
	if atomic.LoadInt32(&entry.refCount) == 0 && entry.stmt != nil {
		_ = entry.stmt.Close()
	}
}

// release decrements the ref count and closes if evicted and unused.
func (c *StmtCache) release(entry *cacheEntry) {
	newCount := atomic.AddInt32(&entry.refCount, -1)
	if newCount == 0 {
		c.mu.Lock()
		defer c.mu.Unlock()
		// Double check under lock (though evicted/refCount logic is mostly safe, closing should be consistent)
		// If it was evicted while we used it, we must close it now.
		if entry.evicted && atomic.LoadInt32(&entry.refCount) == 0 && entry.stmt != nil {
			_ = entry.stmt.Close()
		}
	}
}

// Clear closes all cached statements and clears the cache.
func (c *StmtCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, entry := range c.items {
		entry.evicted = true
		if atomic.LoadInt32(&entry.refCount) == 0 && entry.stmt != nil {
			_ = entry.stmt.Close()
		}
	}

	c.items = make(map[string]*cacheEntry)
	c.lruList.Init()
}

// Close closes all cached statements and releases all resources.
func (c *StmtCache) Close() error {
	c.Clear()
	return nil
}

// Len returns the current number of cached statements.
func (c *StmtCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}
