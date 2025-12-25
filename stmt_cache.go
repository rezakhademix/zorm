package zorm

import (
	"container/list"
	"database/sql"
	"sync"
)

// StmtCache provides a thread-safe LRU cache for prepared statements.
// It stores prepared SQL statements and automatically evicts the least
// recently used entries when the cache reaches its maximum capacity.
//
// The cache is safe for concurrent use by multiple goroutines and helps
// improve performance by reusing prepared statements instead of re-preparing
// them on every execution.
type StmtCache struct {
	mu       sync.RWMutex
	capacity int
	items    map[string]*cacheEntry
	lruList  *list.List
}

// cacheEntry represents a cached prepared statement with its LRU tracking element.
type cacheEntry struct {
	stmt    *sql.Stmt
	element *list.Element
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
// Returns nil if the statement is not found in the cache.
// Accessing a statement updates its position in the LRU list.
func (c *StmtCache) Get(query string) *sql.Stmt {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, exists := c.items[query]; exists {
		c.lruList.MoveToFront(entry.element)
		return entry.stmt
	}

	return nil
}

// Put stores a prepared statement in the cache for the given SQL query.
// If the cache is at capacity, the least recently used statement will be
// closed and evicted before adding the new statement.
//
// If a statement with the same query already exists, it will be replaced.
func (c *StmtCache) Put(query string, stmt *sql.Stmt) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If entry already exists, update it and move to front
	if entry, exists := c.items[query]; exists {
		// Close old statement
		if entry.stmt != nil {
			_ = entry.stmt.Close()
		}
		// Update with new statement
		entry.stmt = stmt
		c.lruList.MoveToFront(entry.element)
		return
	}

	// Evict LRU entry if at capacity
	if len(c.items) >= c.capacity {
		c.evictLRU()
	}

	// Add new entry
	element := c.lruList.PushFront(query)
	c.items[query] = &cacheEntry{
		stmt:    stmt,
		element: element,
	}
}

// evictLRU removes and closes the least recently used statement from the cache.
// This method is not thread-safe and should only be called while holding the write lock.
func (c *StmtCache) evictLRU() {
	// Get the least recently used entry (back of list)
	element := c.lruList.Back()
	if element == nil {
		return
	}

	// Remove from list
	c.lruList.Remove(element)

	// Get query and entry
	query := element.Value.(string)
	entry := c.items[query]

	// Close the statement
	if entry.stmt != nil {
		_ = entry.stmt.Close()
	}

	// Remove from map
	delete(c.items, query)
}

// Clear closes all cached statements and clears the cache.
// This is useful when you want to reset the cache without destroying it.
func (c *StmtCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Close all statements
	for _, entry := range c.items {
		if entry.stmt != nil {
			_ = entry.stmt.Close()
		}
	}

	// Clear the cache
	c.items = make(map[string]*cacheEntry)
	c.lruList.Init()
}

// Close closes all cached statements and releases all resources.
// After calling Close, the cache should not be used anymore.
func (c *StmtCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Close all statements
	for _, entry := range c.items {
		if entry.stmt != nil {
			_ = entry.stmt.Close()
		}
	}

	// Clear the cache
	c.items = nil
	c.lruList = nil

	return nil
}

// Len returns the current number of cached statements.
// This is primarily useful for testing and monitoring.
func (c *StmtCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}
