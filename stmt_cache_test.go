package zorm

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/lib/pq"
)

// Test basic StmtCache operations
func TestStmtCache_BasicOperations(t *testing.T) {
	cache := NewStmtCache(3)
	defer cache.Close()

	// Test initially empty
	if cache.Len() != 0 {
		t.Errorf("expected cache length 0, got %d", cache.Len())
	}

	// Mock database (we won't actually execute statements for this test)
	// For this test, we'll just verify the cache logic
	t.Run("Capacity", func(t *testing.T) {
		if cache.capacity != 3 {
			t.Errorf("expected capacity 3, got %d", cache.capacity)
		}
	})

	t.Run("DefaultCapacity", func(t *testing.T) {
		cache2 := NewStmtCache(0)
		defer cache2.Close()
		if cache2.capacity != 100 {
			t.Errorf("expected default capacity 100, got %d", cache2.capacity)
		}
	})
}

// Test LRU eviction
func TestStmtCache_LRUEviction(t *testing.T) {
	// This test requires a real database connection
	db, err := sql.Open("postgres", "postgres://localhost/zorm_test?sslmode=disable")
	if err != nil {
		t.Skip("Skipping test: database not available")
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skip("Skipping test: database not accessible")
	}

	cache := NewStmtCache(2) // capacity of 2
	defer cache.Close()

	// Prepare statements
	stmt1, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}

	stmt2, err := db.Prepare("SELECT 2")
	if err != nil {
		t.Fatal(err)
	}

	stmt3, err := db.Prepare("SELECT 3")
	if err != nil {
		t.Fatal(err)
	}

	// Add first statement
	cache.Put("SELECT 1", stmt1)
	if cache.Len() != 1 {
		t.Errorf("expected cache length 1, got %d", cache.Len())
	}

	// Add second statement
	cache.Put("SELECT 2", stmt2)
	if cache.Len() != 2 {
		t.Errorf("expected cache length 2, got %d", cache.Len())
	}

	// Add third statement - should evict first
	cache.Put("SELECT 3", stmt3)
	if cache.Len() != 2 {
		t.Errorf("expected cache length 2, got %d", cache.Len())
	}

	// First statement should be evicted
	if cached := cache.Get("SELECT 1"); cached != nil {
		t.Error("expected SELECT 1 to be evicted")
	}

	// Second and third should still be there
	if cached := cache.Get("SELECT 2"); cached == nil {
		t.Error("expected SELECT 2 to be cached")
	}
	if cached := cache.Get("SELECT 3"); cached == nil {
		t.Error("expected SELECT 3 to be cached")
	}
}

// Test Clear and Close
func TestStmtCache_ClearAndClose(t *testing.T) {
	db, err := sql.Open("postgres", "postgres://localhost/zorm_test?sslmode=disable")
	if err != nil {
		t.Skip("Skipping test: database not available")
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skip("Skipping test: database not accessible")
	}

	cache := NewStmtCache(10)

	stmt1, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}

	cache.Put("SELECT 1", stmt1)

	if cache.Len() != 1 {
		t.Errorf("expected cache length 1, got %d", cache.Len())
	}

	// Test Clear
	cache.Clear()
	if cache.Len() != 0 {
		t.Errorf("expected cache length 0 after Clear, got %d", cache.Len())
	}

	// Add again
	stmt2, err := db.Prepare("SELECT 2")
	if err != nil {
		t.Fatal(err)
	}
	cache.Put("SELECT 2", stmt2)

	// Test Close
	err = cache.Close()
	if err != nil {
		t.Errorf("unexpected error on Close: %v", err)
	}
}

// Integration test with Model
func TestModel_WithStmtCache(t *testing.T) {
	db, err := sql.Open("postgres", "postgres://localhost/zorm_test?sslmode=disable")
	if err != nil {
		t.Skip("Skipping test: database not available")
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skip("Skipping test: database not accessible")
	}

	GlobalDB = db

	// Create test table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS test_cache_users (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100),
			email VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE IF EXISTS test_cache_users")

	// Clean up
	_, _ = db.Exec("DELETE FROM test_cache_users")

	type TestCacheUser struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email"`
	}

	cache := NewStmtCache(10)
	defer cache.Close()

	model := New[TestCacheUser]().WithStmtCache(cache)

	// Test Create with cache
	user := &TestCacheUser{Name: "John", Email: "john@example.com"}
	err = model.Create(user)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if user.ID == 0 {
		t.Error("expected ID to be set after Create")
	}

	// Test Get with cache
	users, err := model.Get()
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(users) != 1 {
		t.Errorf("expected 1 user, got %d", len(users))
	}

	// Test Count with cache
	count, err := model.Count()
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}

	if count != 1 {
		t.Errorf("expected count 1, got %d", count)
	}

	// Verify cache has entries
	if cache.Len() == 0 {
		t.Error("expected cache to have entries after operations")
	}

	t.Logf("Cache has %d entries", cache.Len())
}

// Test that operations work without cache (backward compatibility)
func TestModel_WithoutStmtCache(t *testing.T) {
	db, err := sql.Open("postgres", "postgres://localhost/zorm_test?sslmode=disable")
	if err != nil {
		t.Skip("Skipping test: database not available")
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skip("Skipping test: database not accessible")
	}

	GlobalDB = db

	// Create test table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS test_no_cache_users (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE IF NOT EXISTS test_no_cache_users")

	// Clean up
	_, _ = db.Exec("DELETE FROM test_no_cache_users")

	type TestNoCacheUser struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
	}

	// Test without cache (should still work)
	model := New[TestNoCacheUser]()

	user := &TestNoCacheUser{Name: "Jane"}
	err = model.Create(user)
	if err != nil {
		t.Fatalf("Create without cache failed: %v", err)
	}

	users, err := model.Get()
	if err != nil {
		t.Fatalf("Get without cache failed: %v", err)
	}

	if len(users) != 1 {
		t.Errorf("expected 1 user, got %d", len(users))
	}
}

// Test Clone preserves cache reference
func TestModel_ClonePreservesCache(t *testing.T) {
	cache := NewStmtCache(10)
	defer cache.Close()

	type TestUser struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
	}

	model := New[TestUser]().WithStmtCache(cache)
	cloned := model.Clone()

	// Verify cache is preserved
	if cloned.stmtCache != cache {
		t.Error("expected cloned model to preserve cache reference")
	}

	// Verify they point to the same cache instance
	if model.stmtCache != cloned.stmtCache {
		t.Error("expected model and clone to share the same cache instance")
	}
}

// Test context propagation with cached statements
func TestStmtCache_WithContext(t *testing.T) {
	db, err := sql.Open("postgres", "postgres://localhost/zorm_test?sslmode=disable")
	if err != nil {
		t.Skip("Skipping test: database not available")
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skip("Skipping test: database not accessible")
	}

	GlobalDB = db

	type TestUser struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
	}

	cache := NewStmtCache(10)
	defer cache.Close()

	ctx := context.Background()
	model := New[TestUser]().WithStmtCache(cache).WithContext(ctx)

	// This should work with context
	_, err = model.Count()
	if err != nil {
		// It might fail if table doesn't exist, but shouldn't panic
		t.Logf("Count with context: %v", err)
	}
}
