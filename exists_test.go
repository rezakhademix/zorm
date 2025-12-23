package zorm

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// TestExists_Signature verifies that Exists method exists and returns the correct type.
func TestExists_Signature(t *testing.T) {
	m := New[TestModel]()

	// Verify the method signature is correct by type checking
	var _ func(context.Context) (bool, error) = m.Exists

	t.Log("Exists method exists with correct signature")
}

// TestExists_QueryLogic simulates what Exists does to verify logic without executing.
// This duplicates the logic in Exists to ensure our understanding of what it SHOULD do is correct.
func TestExists_QueryLogic(t *testing.T) {
	m := New[TestModel]().Where("id", 1)

	// Replicate Exists logic for query building verification
	limit, offset := m.limit, m.offset
	orderBys := m.orderBys

	m.limit = 1
	m.offset = 0
	m.orderBys = nil

	sb := GetStringBuilder()
	defer PutStringBuilder(sb)

	sb.WriteString("SELECT 1 FROM ")
	sb.WriteString(m.TableName())
	m.buildWhereClause(sb)
	sb.WriteString(" LIMIT 1")

	query := sb.String()

	// Restore
	m.limit, m.offset = limit, offset
	m.orderBys = orderBys

	// Expected query based on zorm's query builder (double space is from implementation artifact)
	expected := "SELECT 1 FROM test_models WHERE 1=1  AND id = ? LIMIT 1"
	if query != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
}

// TestExists_Execution verifies Exists works against a real DB (sqlite in-memory).
func TestExists_Execution(t *testing.T) {
	// Initialize in-memory DB
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(`
		CREATE TABLE test_models (
			id INTEGER PRIMARY KEY,
			name TEXT,
			user_age INTEGER,
			embedded_field TEXT
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert data
	_, err = db.Exec(`INSERT INTO test_models (id, name, user_age) VALUES (1, 'Alice', 30)`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Setup zorm global DB (or use instance)
	// We'll use a specific instance to avoid messing with global state if other tests run in parallel
	m := New[TestModel]()
	m.SetDB(db)

	// Test case 1: Record exists
	exists, err := m.Where("id", 1).Exists(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if !exists {
		t.Error("expected exists to be true for id 1")
	}

	// Test case 2: Record does not exist
	exists, err = New[TestModel]().SetDB(db).Where("id", 999).Exists(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if exists {
		t.Error("expected exists to be false for id 999")
	}
}

// TestExists_StmtCache verifies Exists works with statement caching enabled.
func TestExists_StmtCache(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test_models (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO test_models (id) VALUES (1)")
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Create model with stmt cache
	cache := NewStmtCache(10)
	defer cache.Close()

	m := New[TestModel]().SetDB(db).WithStmtCache(cache)

	// First run (cache miss -> prepare -> exec)
	exists, err := m.Where("id", 1).Exists(context.Background())
	if err != nil {
		t.Errorf("expected no error on first run, got %v", err)
	}
	if !exists {
		t.Error("expected exists to be true")
	}

	// Verify it's in cache (though we can't inspect cache easily without locking, we trust the code path if no error)

	// Second run (cache hit -> exec)
	exists, err = m.Where("id", 1).Exists(context.Background())
	if err != nil {
		t.Errorf("expected no error on second run, got %v", err)
	}
	if !exists {
		t.Error("expected exists to be true")
	}
}

// TestExists_Error verifies proper error handling.
func TestExists_Error(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	// Close early to trigger execution error
	db.Close()

	m := New[TestModel]().SetDB(db)

	_, err = m.Where("id", 1).Exists(context.Background())
	if err == nil {
		t.Error("expected error due to closed db, got nil")
	}

	// Test with cache enabled and closed DB (covers Prepare error path)
	// Re-open DB to get a fresh closed state properly or just reuse broken one if safe?
	// The DB is already closed, so reuse is fine for generating errors.

	cache := NewStmtCache(10)
	defer cache.Close()
	mWithCache := New[TestModel]().SetDB(db).WithStmtCache(cache)

	_, err = mWithCache.Where("id", 1).Exists(context.Background())
	if err == nil {
		t.Error("expected error with cache enabled due to closed db, got nil")
	}
}
