package zorm

import (
	"fmt"
	"sync"
	"testing"
)

func TestStmtCache_Concurrency(t *testing.T) {
	cache := NewStmtCache(10)
	var wg sync.WaitGroup

	// Simulate concurrent access
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(wrapperID int) {
			defer wg.Done()
			query := fmt.Sprintf("SELECT * FROM table WHERE id = %d", wrapperID%20) // Reuse some queries

			// Simulate Get
			// Note: In real usage, Get returns *sql.Stmt.
			// Here we just check logic safety, we can't fully mock sql.Stmt without valid DB.
			// But we can test Put/Get concurrent safety if we could put something.
			// Since sql.Stmt is opaque struct, we can pass nil for testing if StmtCache allows it?
			// Put(query, nil) -> entry.stmt = nil. Get returns nil.
			// But StmtCache logic is generic regarding *sql.Stmt content.

			cache.Put(query, nil)
			_ = cache.Get(query)

		}(i)
	}

	wg.Wait()

	if cache.Len() > 10 {
		t.Errorf("Cache capacity exceeded: got %d, expected <= 10", cache.Len())
	}
}
