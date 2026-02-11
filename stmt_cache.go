package zorm

import (
	"container/list"
	"database/sql"
	"hash/fnv"
	"sync"
	"sync/atomic"
)

// stmtShardCount is the number of shards for the statement cache.
// Using 64 shards provides good distribution while keeping memory overhead low.
const stmtShardCount = 64

// StmtCache provides a thread-safe LRU cache for prepared statements.
// It stores prepared SQL statements and automatically evicts the least
// recently used entries when the cache reaches its maximum capacity.
//
// The cache uses sharded locking to reduce contention under high concurrency.
// It is safe for concurrent use by multiple goroutines and helps
// improve performance by reusing prepared statements instead of re-preparing
// them on every execution.
type StmtCache struct {
	shards   [stmtShardCount]*stmtCacheShard
	capacity int
	closed   atomic.Bool // Set to true after Close/Clear to signal release() to close stmts directly
}

// stmtCacheShard represents a single shard of the cache with its own lock.
type stmtCacheShard struct {
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

	// Distribute capacity across shards
	shardCapacity := capacity / stmtShardCount
	if shardCapacity < 1 {
		shardCapacity = 1
	}

	c := &StmtCache{
		capacity: capacity,
	}

	for i := 0; i < stmtShardCount; i++ {
		c.shards[i] = &stmtCacheShard{
			capacity: shardCapacity,
			items:    make(map[string]*cacheEntry),
			lruList:  list.New(),
		}
	}

	return c
}

// getShard returns the shard for the given query using FNV-1a hash.
func (c *StmtCache) getShard(query string) *stmtCacheShard {
	h := fnv.New32a()
	h.Write([]byte(query))
	return c.shards[h.Sum32()%stmtShardCount]
}

// Get retrieves a cached prepared statement for the given SQL query.
// Returns the statement and a release function. The caller MUST call the release function
// when finished using the statement.
// Returns nil, nil if the statement is not found in the cache.
func (c *StmtCache) Get(query string) (*sql.Stmt, func()) {
	shard := c.getShard(query)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if entry, exists := shard.items[query]; exists {
		shard.lruList.MoveToFront(entry.element)
		atomic.AddInt32(&entry.refCount, 1)
		return entry.stmt, func() {
			c.release(shard, entry)
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
	shard := c.getShard(query)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	c.putLocked(shard, query, stmt)
}

// putLocked is the internal implementation of Put, assumes lock is held.
func (c *StmtCache) putLocked(shard *stmtCacheShard, query string, stmt *sql.Stmt) *cacheEntry {
	// If entry already exists, update it?
	// It's tricky to update an existing entry safely if it's in use.
	// We should evict the old one and add new one.
	if entry, exists := shard.items[query]; exists {
		c.evictEntry(shard, entry)
	}

	// Evict LRU entry if at capacity
	if len(shard.items) >= shard.capacity {
		c.evictLRU(shard)
	}

	entry := &cacheEntry{
		stmt:     stmt,
		query:    query,
		refCount: 0,
		evicted:  false,
	}
	element := shard.lruList.PushFront(entry)
	entry.element = element
	shard.items[query] = entry
	return entry
}

// PutAndGet atomically stores a prepared statement and retrieves it with
// an incremented reference count. This avoids race conditions where the
// statement could be evicted between Put and Get calls.
// Returns the statement and a release function. The caller MUST call the release function.
func (c *StmtCache) PutAndGet(query string, stmt *sql.Stmt) (*sql.Stmt, func()) {
	shard := c.getShard(query)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry := c.putLocked(shard, query, stmt)
	atomic.AddInt32(&entry.refCount, 1)
	return entry.stmt, func() {
		c.release(shard, entry)
	}
}

// evictLRU removes the least recently used statement from the cache.
func (c *StmtCache) evictLRU(shard *stmtCacheShard) {
	element := shard.lruList.Back()
	if element == nil {
		return
	}
	entry := element.Value.(*cacheEntry)
	c.evictEntry(shard, entry)
}

// evictEntry removes an entry from the map and list, marking it as evicted.
func (c *StmtCache) evictEntry(shard *stmtCacheShard, entry *cacheEntry) {
	shard.lruList.Remove(entry.element)
	delete(shard.items, entry.query)
	entry.evicted = true

	// If no one is using it, close immediately
	if atomic.LoadInt32(&entry.refCount) == 0 && entry.stmt != nil {
		_ = entry.stmt.Close()
	}
}

// release decrements the ref count and closes if evicted and unused.
// The lock must be acquired BEFORE decrementing refCount to prevent a TOCTOU race
// where another goroutine could evict and close the stmt between our decrement
// and our lock acquisition.
func (c *StmtCache) release(shard *stmtCacheShard, entry *cacheEntry) {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	newCount := atomic.AddInt32(&entry.refCount, -1)
	// If refCount is now 0 and it was evicted (or cache was closed) while we were using it, close it.
	if newCount == 0 && (entry.evicted || c.closed.Load()) && entry.stmt != nil {
		_ = entry.stmt.Close()
		entry.stmt = nil // Prevent double-close
	}
}

// Clear closes all cached statements and clears the cache.
func (c *StmtCache) Clear() {
	for i := 0; i < stmtShardCount; i++ {
		shard := c.shards[i]
		shard.mu.Lock()

		for _, entry := range shard.items {
			entry.evicted = true
			if atomic.LoadInt32(&entry.refCount) == 0 && entry.stmt != nil {
				_ = entry.stmt.Close()
			}
		}

		shard.items = make(map[string]*cacheEntry)
		shard.lruList.Init()
		shard.mu.Unlock()
	}
}

// Close closes all cached statements and releases all resources.
// In-flight statements (with refCount > 0) will be closed when their
// release function is called after the last user is done.
func (c *StmtCache) Close() error {
	c.closed.Store(true)
	c.Clear()
	return nil
}

// Len returns the current number of cached statements.
func (c *StmtCache) Len() int {
	total := 0
	for i := 0; i < stmtShardCount; i++ {
		shard := c.shards[i]
		shard.mu.Lock()
		total += len(shard.items)
		shard.mu.Unlock()
	}
	return total
}
