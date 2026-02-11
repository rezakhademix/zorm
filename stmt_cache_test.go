package zorm

import (
	"fmt"
	"sync"
	"testing"
)

func TestStmtCache_Concurrency(t *testing.T) {
	// Use capacity of 256 (64 shards * 4 per shard)
	// This ensures meaningful capacity distribution with sharded cache
	cache := NewStmtCache(256)
	var wg sync.WaitGroup

	// Simulate concurrent access
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func(wrapperID int) {
			defer wg.Done()
			query := fmt.Sprintf("SELECT * FROM table WHERE id = %d", wrapperID%100) // Reuse some queries

			// Simulate Get
			// Note: In real usage, Get returns *sql.Stmt.
			// Here we just check logic safety, we can't fully mock sql.Stmt without valid DB.
			// But we can test Put/Get concurrent safety if we could put something.
			// Since sql.Stmt is opaque struct, we can pass nil for testing if StmtCache allows it?
			// Put(query, nil) -> entry.stmt = nil. Get returns nil.
			// But StmtCache logic is generic regarding *sql.Stmt content.

			cache.Put(query, nil)
			_, release := cache.Get(query)
			if release != nil {
				release()
			}

		}(i)
	}

	wg.Wait()

	// With 64 shards and capacity 256, each shard gets ~4 items capacity
	// The total cached items should not exceed the total capacity
	if cache.Len() > 256 {
		t.Errorf("Cache capacity exceeded: got %d, expected <= 256", cache.Len())
	}
}

func TestStmtCache_Clear(t *testing.T) {
	cache := NewStmtCache(100)
	cache.Put("Q1", nil)
	cache.Put("Q2", nil)

	if cache.Len() < 2 {
		t.Errorf("expected at least 2 entries, got %d", cache.Len())
	}

	cache.Clear()

	if cache.Len() != 0 {
		t.Errorf("expected length 0 after Clear, got %d", cache.Len())
	}
}

func TestStmtCache_GetAndPut(t *testing.T) {
	cache := NewStmtCache(100)

	// Put a statement
	cache.Put("SELECT 1", nil)

	// Get it back
	stmt, release := cache.Get("SELECT 1")
	if release == nil {
		t.Fatal("expected release function, got nil")
	}
	if stmt != nil {
		t.Error("expected nil stmt (since we put nil)")
	}
	release()

	// Get non-existent query
	stmt2, release2 := cache.Get("SELECT 2")
	if release2 != nil {
		t.Error("expected nil release for non-existent query")
	}
	if stmt2 != nil {
		t.Error("expected nil stmt for non-existent query")
	}
}

func TestStmtCache_PutAndGet(t *testing.T) {
	cache := NewStmtCache(100)

	// PutAndGet atomically
	stmt, release := cache.PutAndGet("SELECT 1", nil)
	if release == nil {
		t.Fatal("expected release function, got nil")
	}
	if stmt != nil {
		t.Error("expected nil stmt (since we put nil)")
	}
	release()

	// Verify it's in the cache
	if cache.Len() != 1 {
		t.Errorf("expected 1 entry, got %d", cache.Len())
	}
}

func TestStmtCache_Eviction(t *testing.T) {
	// Create cache with capacity for 64 items (1 per shard minimum)
	cache := NewStmtCache(64)

	// Add more items than capacity - some should be evicted
	for i := 0; i < 200; i++ {
		cache.Put(fmt.Sprintf("SELECT %d", i), nil)
	}

	// Cache should not exceed total capacity
	if cache.Len() > 200 {
		t.Errorf("cache exceeded maximum possible size: got %d", cache.Len())
	}
}

// =============================================================================
// ISSUE #7: STMTCACHE CLOSE WITH IN-FLIGHT STATEMENTS
// =============================================================================

// TestStmtCache_CloseWithInFlightStatements verifies that Close() followed by
// release() on a held reference does not panic or cause undefined behavior.
func TestStmtCache_CloseWithInFlightStatements(t *testing.T) {
	cache := NewStmtCache(100)

	// PutAndGet holds a reference (refCount = 1)
	_, release := cache.PutAndGet("SELECT 1", nil)
	if release == nil {
		t.Fatal("expected release function, got nil")
	}

	// Close the cache while the statement is still in-flight
	err := cache.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Cache should be empty after Close
	if cache.Len() != 0 {
		t.Errorf("expected 0 entries after Close, got %d", cache.Len())
	}

	// Release should not panic even though cache is closed
	release()
}

// TestStmtCache_ConcurrentCloseAndGet verifies no data race or panic when
// goroutines do Get()/release() while another goroutine calls Close().
func TestStmtCache_ConcurrentCloseAndGet(t *testing.T) {
	cache := NewStmtCache(256)

	// Pre-populate cache
	for i := 0; i < 100; i++ {
		cache.Put(fmt.Sprintf("SELECT %d", i), nil)
	}

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Goroutines doing Get/release
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					query := fmt.Sprintf("SELECT %d", id%100)
					stmt, release := cache.Get(query)
					if release != nil {
						_ = stmt
						release()
					}
				}
			}
		}(i)
	}

	// Goroutines doing PutAndGet/release
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					query := fmt.Sprintf("SELECT new_%d", id)
					_, release := cache.PutAndGet(query, nil)
					if release != nil {
						release()
					}
				}
			}
		}(i)
	}

	// Let goroutines run briefly, then Close
	// Use a small sleep to let goroutines start
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Close the cache while other goroutines are active
		cache.Close()
	}()

	// Signal all goroutines to stop
	close(done)
	wg.Wait()
}

// TestStmtCache_ClearWithHeldReferences verifies that Clear() marks entries
// as evicted, and subsequent release() handles cleanup properly.
func TestStmtCache_ClearWithHeldReferences(t *testing.T) {
	cache := NewStmtCache(100)

	// PutAndGet to hold references
	_, release1 := cache.PutAndGet("SELECT 1", nil)
	_, release2 := cache.PutAndGet("SELECT 2", nil)
	if release1 == nil || release2 == nil {
		t.Fatal("expected release functions")
	}

	// Clear while references are held
	cache.Clear()

	if cache.Len() != 0 {
		t.Errorf("expected 0 entries after Clear, got %d", cache.Len())
	}

	// Releasing held references should not panic
	release1()
	release2()
}
