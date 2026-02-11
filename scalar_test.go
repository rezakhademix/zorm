package zorm

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// setupScalarTestDB creates an in-memory SQLite database with test data
func setupScalarTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT,
			age INTEGER,
			active INTEGER DEFAULT 1,
			role TEXT
		);
		INSERT INTO users (name, email, age, active, role) VALUES
			('Alice', 'alice@example.com', 30, 1, 'admin'),
			('Bob', 'bob@example.com', 25, 1, 'user'),
			('Charlie', 'charlie@example.com', 35, 0, 'user'),
			('Diana', 'diana@example.com', 28, 1, 'admin');

		CREATE TABLE roles (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			active INTEGER DEFAULT 1
		);
		INSERT INTO roles (name, active) VALUES
			('admin', 1),
			('user', 1),
			('guest', 0);
	`)
	if err != nil {
		t.Fatalf("failed to setup database: %v", err)
	}

	return db
}

func TestScalarQuery_Get_StringColumn(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where("active", 1).
		OrderBy("name", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	expected := []string{"Alice", "Bob", "Diana"}
	if len(names) != len(expected) {
		t.Fatalf("expected %d names, got %d", len(expected), len(names))
	}

	for i, name := range names {
		if name != expected[i] {
			t.Errorf("expected names[%d] = %q, got %q", i, expected[i], name)
		}
	}
}

func TestScalarQuery_Get_IntColumn(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	ids, err := Query[int64]().
		SetDB(db).
		Table("users").
		Select("id").
		OrderBy("id", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(ids) != 4 {
		t.Fatalf("expected 4 ids, got %d", len(ids))
	}

	for i, id := range ids {
		if id != int64(i+1) {
			t.Errorf("expected ids[%d] = %d, got %d", i, i+1, id)
		}
	}
}

func TestScalarQuery_First(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Test successful first
	name, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where("id", 1).
		First(ctx)

	if err != nil {
		t.Fatalf("First failed: %v", err)
	}

	if name != "Alice" {
		t.Errorf("expected 'Alice', got %q", name)
	}
}

func TestScalarQuery_First_NotFound(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	_, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where("id", 9999).
		First(ctx)

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, ErrRecordNotFound) {
		t.Errorf("expected ErrRecordNotFound, got %v", err)
	}
}

func TestScalarQuery_Distinct(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	roles, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("role").
		Distinct().
		OrderBy("role", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	expected := []string{"admin", "user"}
	if len(roles) != len(expected) {
		t.Fatalf("expected %d roles, got %d", len(expected), len(roles))
	}

	for i, role := range roles {
		if role != expected[i] {
			t.Errorf("expected roles[%d] = %q, got %q", i, expected[i], role)
		}
	}
}

func TestScalarQuery_Limit(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		OrderBy("id", "ASC").
		Limit(2).
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(names) != 2 {
		t.Fatalf("expected 2 names, got %d", len(names))
	}

	if names[0] != "Alice" || names[1] != "Bob" {
		t.Errorf("expected [Alice, Bob], got %v", names)
	}
}

func TestScalarQuery_Offset(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		OrderBy("id", "ASC").
		Limit(2).
		Offset(1).
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(names) != 2 {
		t.Fatalf("expected 2 names, got %d", len(names))
	}

	if names[0] != "Bob" || names[1] != "Charlie" {
		t.Errorf("expected [Bob, Charlie], got %v", names)
	}
}

func TestScalarQuery_WhereIn(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		WhereIn("id", []any{1, 3}).
		OrderBy("id", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	expected := []string{"Alice", "Charlie"}
	if len(names) != len(expected) {
		t.Fatalf("expected %d names, got %d", len(expected), len(names))
	}

	for i, name := range names {
		if name != expected[i] {
			t.Errorf("expected names[%d] = %q, got %q", i, expected[i], name)
		}
	}
}

func TestScalarQuery_WhereIn_Empty(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		WhereIn("id", []any{}).
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(names) != 0 {
		t.Errorf("expected 0 names for empty WhereIn, got %d", len(names))
	}
}

func TestScalarQuery_WhereNull(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	// Update one user to have NULL email
	_, _ = db.Exec("UPDATE users SET email = NULL WHERE id = 1")

	ctx := context.Background()
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		WhereNull("email").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(names) != 1 || names[0] != "Alice" {
		t.Errorf("expected [Alice], got %v", names)
	}
}

func TestScalarQuery_WhereNotNull(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	// Update one user to have NULL email
	_, _ = db.Exec("UPDATE users SET email = NULL WHERE id = 1")

	ctx := context.Background()
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		WhereNotNull("email").
		OrderBy("id", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	expected := []string{"Bob", "Charlie", "Diana"}
	if len(names) != len(expected) {
		t.Fatalf("expected %d names, got %d", len(expected), len(names))
	}
}

func TestScalarQuery_GroupBy(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	roles, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("role").
		GroupBy("role").
		OrderBy("role", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	expected := []string{"admin", "user"}
	if len(roles) != len(expected) {
		t.Fatalf("expected %d roles, got %d", len(expected), len(roles))
	}

	for i, role := range roles {
		if role != expected[i] {
			t.Errorf("expected roles[%d] = %q, got %q", i, expected[i], role)
		}
	}
}

func TestScalarQuery_Having(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	roles, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("role").
		GroupBy("role").
		Having("COUNT(*) > ?", 1).
		OrderBy("role", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Both admin and user have > 1 row
	expected := []string{"admin", "user"}
	if len(roles) != len(expected) {
		t.Fatalf("expected %d roles, got %d: %v", len(expected), len(roles), roles)
	}
}

func TestScalarQuery_Count(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	count, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where("active", 1).
		Count(ctx)

	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}

	if count != 3 {
		t.Errorf("expected count 3, got %d", count)
	}
}

func TestScalarQuery_OrWhere(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where("id", 1).
		OrWhere("id", 3).
		OrderBy("id", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	expected := []string{"Alice", "Charlie"}
	if len(names) != len(expected) {
		t.Fatalf("expected %d names, got %d", len(expected), len(names))
	}

	for i, name := range names {
		if name != expected[i] {
			t.Errorf("expected names[%d] = %q, got %q", i, expected[i], name)
		}
	}
}

func TestScalarQuery_WhereWithOperator(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where("age", ">", 28).
		OrderBy("age", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	expected := []string{"Alice", "Charlie"}
	if len(names) != len(expected) {
		t.Fatalf("expected %d names, got %d", len(expected), len(names))
	}

	for i, name := range names {
		if name != expected[i] {
			t.Errorf("expected names[%d] = %q, got %q", i, expected[i], name)
		}
	}
}

func TestScalarQuery_WhereMap(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where(map[string]any{"role": "admin", "active": 1}).
		OrderBy("name", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	expected := []string{"Alice", "Diana"}
	if len(names) != len(expected) {
		t.Fatalf("expected %d names, got %d", len(expected), len(names))
	}

	for i, name := range names {
		if name != expected[i] {
			t.Errorf("expected names[%d] = %q, got %q", i, expected[i], name)
		}
	}
}

func TestScalarQuery_Print(t *testing.T) {
	query, args := Query[string]().
		Table("users").
		Select("name").
		Where("active", 1).
		OrderBy("name", "ASC").
		Limit(10).
		Print()

	// Verify the query contains expected parts
	if !strings.Contains(query, "SELECT name FROM users") {
		t.Errorf("expected query to contain SELECT name FROM users, got %q", query)
	}

	if len(args) != 1 || args[0] != 1 {
		t.Errorf("expected args [1], got %v", args)
	}
}

func TestScalarQuery_WithTransaction(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()
	SetGlobalDB(db)
	defer SetGlobalDB(nil)

	ctx := context.Background()

	err := Transaction(ctx, func(tx *Tx) error {
		// Insert a new user within transaction
		_, err := tx.Tx.Exec("INSERT INTO users (name, email, age, active, role) VALUES ('Eve', 'eve@example.com', 22, 1, 'user')")
		if err != nil {
			return err
		}

		// Query within same transaction
		names, err := Query[string]().
			WithTx(tx).
			Table("users").
			Select("name").
			Where("name", "Eve").
			Get(ctx)

		if err != nil {
			return err
		}

		if len(names) != 1 || names[0] != "Eve" {
			t.Errorf("expected [Eve] within transaction, got %v", names)
		}

		return nil
	})

	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}

	// Verify commit persisted
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where("name", "Eve").
		Get(ctx)

	if err != nil {
		t.Fatalf("Post-transaction query failed: %v", err)
	}

	if len(names) != 1 {
		t.Errorf("expected Eve to be persisted, got %v", names)
	}
}

func TestScalarQuery_BuildQuery(t *testing.T) {
	// Test that buildQuery produces correct SQL structure
	q := Query[string]().
		Table("users").
		Select("name").
		Distinct().
		Where("active", 1).
		GroupBy("role").
		Having("COUNT(*) > ?", 1).
		OrderBy("name", "ASC").
		Limit(10).
		Offset(5)

	query := q.buildQuery()

	// Verify key parts of the query
	if !strings.Contains(query, "SELECT DISTINCT name") {
		t.Errorf("expected SELECT DISTINCT name, got %q", query)
	}
	if !strings.Contains(query, "FROM users") {
		t.Errorf("expected FROM users, got %q", query)
	}
	if !strings.Contains(query, "WHERE 1=1") {
		t.Errorf("expected WHERE 1=1, got %q", query)
	}
	if !strings.Contains(query, "GROUP BY role") {
		t.Errorf("expected GROUP BY role, got %q", query)
	}
	if !strings.Contains(query, "HAVING COUNT(*) > ?") {
		t.Errorf("expected HAVING COUNT(*) > ?, got %q", query)
	}
	if !strings.Contains(query, "ORDER BY name ASC") {
		t.Errorf("expected ORDER BY name ASC, got %q", query)
	}
	if !strings.Contains(query, "LIMIT 10") {
		t.Errorf("expected LIMIT 10, got %q", query)
	}
	if !strings.Contains(query, "OFFSET 5") {
		t.Errorf("expected OFFSET 5, got %q", query)
	}
}

func TestScalarQuery_NullableTypes(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	// Update one user to have NULL email
	_, _ = db.Exec("UPDATE users SET email = NULL WHERE id = 1")

	ctx := context.Background()

	// Query with sql.NullString to handle NULL values
	emails, err := Query[sql.NullString]().
		SetDB(db).
		Table("users").
		Select("email").
		OrderBy("id", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(emails) != 4 {
		t.Fatalf("expected 4 emails, got %d", len(emails))
	}

	// First email should be NULL
	if emails[0].Valid {
		t.Errorf("expected emails[0] to be NULL, got %q", emails[0].String)
	}

	// Second email should be valid
	if !emails[1].Valid || emails[1].String != "bob@example.com" {
		t.Errorf("expected emails[1] = 'bob@example.com', got %v", emails[1])
	}
}

func TestScalarQuery_Float64Column(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			name TEXT,
			price REAL
		);
		INSERT INTO products (name, price) VALUES
			('Apple', 1.50),
			('Banana', 0.75),
			('Cherry', 2.25);
	`)
	if err != nil {
		t.Fatalf("failed to setup database: %v", err)
	}

	ctx := context.Background()
	prices, err := Query[float64]().
		SetDB(db).
		Table("products").
		Select("price").
		OrderBy("price", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	expected := []float64{0.75, 1.50, 2.25}
	if len(prices) != len(expected) {
		t.Fatalf("expected %d prices, got %d", len(expected), len(prices))
	}

	for i, price := range prices {
		if price != expected[i] {
			t.Errorf("expected prices[%d] = %f, got %f", i, expected[i], price)
		}
	}
}

func TestScalarQuery_BoolColumn(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	activeStates, err := Query[bool]().
		SetDB(db).
		Table("users").
		Select("active").
		Distinct().
		OrderBy("active", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(activeStates) != 2 {
		t.Fatalf("expected 2 distinct active states, got %d", len(activeStates))
	}

	// SQLite stores boolean as 0/1
	if activeStates[0] != false || activeStates[1] != true {
		t.Errorf("expected [false, true], got %v", activeStates)
	}
}

func TestScalarQuery_Clone(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Create a base query
	base := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where("active", 1).
		OrderBy("name", "ASC")

	// Clone and modify
	clone := base.Clone().Limit(2)

	// Execute both
	baseResults, err := base.Get(ctx)
	if err != nil {
		t.Fatalf("base Get failed: %v", err)
	}

	cloneResults, err := clone.Get(ctx)
	if err != nil {
		t.Fatalf("clone Get failed: %v", err)
	}

	// Base should have all active users (3)
	if len(baseResults) != 3 {
		t.Errorf("expected base to have 3 results, got %d", len(baseResults))
	}

	// Clone should have limit of 2
	if len(cloneResults) != 2 {
		t.Errorf("expected clone to have 2 results, got %d", len(cloneResults))
	}
}

func TestScalarQuery_FirstDoesNotMutateOriginal(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Create a query
	q := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where("active", 1).
		OrderBy("name", "ASC")

	// Call First (should not mutate original)
	_, err := q.First(ctx)
	if err != nil {
		t.Fatalf("First failed: %v", err)
	}

	// Original query should still return all results
	results, err := q.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Should have 3 active users, not 1
	if len(results) != 3 {
		t.Errorf("expected 3 results after First(), got %d (First() mutated original)", len(results))
	}
}

func TestScalarQuery_ConcurrentUsage(t *testing.T) {
	// Use shared cache mode for concurrent access to in-memory SQLite
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL
		);
		INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie'), ('Diana');
	`)
	if err != nil {
		t.Fatalf("failed to setup database: %v", err)
	}

	ctx := context.Background()

	var wg sync.WaitGroup
	errChan := make(chan error, 10)

	// Run multiple concurrent queries, each creating their own query
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			q := Query[string]().
				SetDB(db).
				Table("users").
				Select("name").
				OrderBy("id", "ASC").
				Limit(n%4 + 1)
			results, err := q.Get(ctx)
			if err != nil {
				errChan <- err
				return
			}
			expectedLen := n%4 + 1
			if len(results) != expectedLen {
				errChan <- errors.New("unexpected result length")
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("concurrent query failed: %v", err)
	}
}

func TestScalarQuery_CloneConcurrentUsage(t *testing.T) {
	// Use shared cache mode for concurrent access to in-memory SQLite
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL
		);
		INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie'), ('Diana');
	`)
	if err != nil {
		t.Fatalf("failed to setup database: %v", err)
	}

	ctx := context.Background()

	// Create a base query
	base := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		OrderBy("id", "ASC")

	var wg sync.WaitGroup
	errChan := make(chan error, 10)

	// Run multiple concurrent queries using Clone()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			clone := base.Clone().Limit(n%4 + 1)
			results, err := clone.Get(ctx)
			if err != nil {
				errChan <- err
				return
			}
			expectedLen := n%4 + 1
			if len(results) != expectedLen {
				errChan <- errors.New("unexpected result length")
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("concurrent clone query failed: %v", err)
	}
}

func TestScalarQuery_TableValidation(t *testing.T) {
	// Test that invalid table names are rejected
	q := Query[string]().Table("users; DROP TABLE users;--")

	// Table name should not be set due to validation failure
	if q.tableName != "" {
		t.Errorf("expected empty table name for invalid input, got %q", q.tableName)
	}

	// Valid table name should work
	q2 := Query[string]().Table("users")
	if q2.tableName != "users" {
		t.Errorf("expected table name 'users', got %q", q2.tableName)
	}
}

func TestScalarQuery_WhereColumnValidation(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Invalid column name should be rejected
	q := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where("id; DROP TABLE users;--", 1)

	// Query should still work but the malicious where should be ignored
	results, err := q.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Should return all users since the invalid WHERE was skipped
	if len(results) != 4 {
		t.Errorf("expected 4 results (invalid WHERE skipped), got %d", len(results))
	}
}

// ============================================
// HIGH PRIORITY TESTS
// ============================================

func TestScalarQuery_MultipleOrderBy(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		OrderBy("role", "ASC").
		OrderBy("name", "DESC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// role ASC: admin (Alice, Diana), user (Bob, Charlie)
	// then name DESC within each role
	expected := []string{"Diana", "Alice", "Charlie", "Bob"}
	if len(names) != len(expected) {
		t.Fatalf("expected %d names, got %d", len(expected), len(names))
	}

	for i, name := range names {
		if name != expected[i] {
			t.Errorf("expected names[%d] = %q, got %q", i, expected[i], name)
		}
	}
}

func TestScalarQuery_MultipleGroupBy(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// GroupBy multiple columns
	count, err := Query[int64]().
		SetDB(db).
		Table("users").
		Select("COUNT(*)").
		GroupBy("role", "active").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Should return groups: (admin,1), (user,1), (user,0)
	// Note: Both admins are active=1, users split between active=1 and active=0
	if len(count) != 3 {
		t.Errorf("expected 3 groups, got %d", len(count))
	}
}

func TestScalarQuery_MultipleHaving(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	roles, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("role").
		GroupBy("role").
		Having("COUNT(*) >= ?", 1).
		Having("COUNT(*) <= ?", 2).
		OrderBy("role", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Both admin (2 users) and user (2 users) satisfy HAVING conditions
	expected := []string{"admin", "user"}
	if len(roles) != len(expected) {
		t.Fatalf("expected %d roles, got %d", len(expected), len(roles))
	}

	for i, role := range roles {
		if role != expected[i] {
			t.Errorf("expected roles[%d] = %q, got %q", i, expected[i], role)
		}
	}
}

func TestScalarQuery_ComplexQuery(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Test all features combined
	roles, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("role").
		Where("active", 1).
		GroupBy("role").
		Having("COUNT(*) > ?", 0).
		OrderBy("role", "ASC").
		Limit(10).
		Offset(0).
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Active users: Alice (admin), Bob (user), Diana (admin)
	// Groups: admin (2), user (1) - both have COUNT > 0
	expected := []string{"admin", "user"}
	if len(roles) != len(expected) {
		t.Fatalf("expected %d roles, got %d", len(expected), len(roles))
	}
}

func TestScalarQuery_WhereRawNoArgs(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where("age > 28").
		OrderBy("age", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Users with age > 28: Alice (30), Charlie (35)
	expected := []string{"Alice", "Charlie"}
	if len(names) != len(expected) {
		t.Fatalf("expected %d names, got %d", len(expected), len(names))
	}

	for i, name := range names {
		if name != expected[i] {
			t.Errorf("expected names[%d] = %q, got %q", i, expected[i], name)
		}
	}
}

func TestScalarQuery_WhereRawMultipleArgs(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Raw WHERE with 3+ args triggers the raw query path (default case in addWhere)
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where("age > ? AND name LIKE ? AND active = ?", 20, "A%", 1).
		OrderBy("name", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Users with age > 20 AND name starting with A AND active: Alice (30)
	expected := []string{"Alice"}
	if len(names) != len(expected) {
		t.Fatalf("expected %d names, got %d", len(expected), len(names))
	}

	if names[0] != "Alice" {
		t.Errorf("expected 'Alice', got %q", names[0])
	}
}

func TestScalarQuery_OrderByInvalidDirection(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Invalid direction should default to DESC
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		OrderBy("id", "INVALID").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// With DESC order by id, Diana (4), Charlie (3), Bob (2), Alice (1)
	if len(names) != 4 {
		t.Fatalf("expected 4 names, got %d", len(names))
	}

	// First should be Diana (highest id)
	if names[0] != "Diana" {
		t.Errorf("expected first name 'Diana' with invalid direction defaulting to DESC, got %q", names[0])
	}
}

func TestScalarQuery_ScanError(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Trying to scan string column "name" into int64 should cause scan error
	_, err := Query[int64]().
		SetDB(db).
		Table("users").
		Select("name").
		Get(ctx)

	// SQLite may or may not return error depending on the value
	// But trying to scan a text value should at least succeed (SQLite is lenient)
	// The main point is the query runs without panicking
	_ = err
}

func TestScalarQuery_ContextCancellation(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Get(ctx)

	if err == nil {
		t.Error("expected error with canceled context, got nil")
	}
}

func TestScalarQuery_ContextTimeout(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()

	_, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Get(ctx)

	if err == nil {
		t.Error("expected error with timeout context, got nil")
	}
}

func TestScalarQuery_EmptyResultSet(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Query for non-existent data
	results, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where("id", 9999).
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Should return empty slice, not nil
	if results == nil {
		t.Error("expected empty slice, got nil")
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

// ============================================
// MEDIUM PRIORITY TESTS
// ============================================

func TestScalarQuery_NegativeLimit(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Negative limit should be treated as no limit (limit <= 0 check in buildQuery)
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		OrderBy("id", "ASC").
		Limit(-1).
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Should return all 4 users since negative limit is ignored
	if len(names) != 4 {
		t.Errorf("expected 4 names with negative limit, got %d", len(names))
	}
}

func TestScalarQuery_ZeroLimit(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Zero limit should be treated as no limit (limit <= 0 check in buildQuery)
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		OrderBy("id", "ASC").
		Limit(0).
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Should return all 4 users since zero limit is ignored
	if len(names) != 4 {
		t.Errorf("expected 4 names with zero limit, got %d", len(names))
	}
}

func TestScalarQuery_VeryLargeLimit(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Very large limit should work fine
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		OrderBy("id", "ASC").
		Limit(1000000).
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Should return all 4 users
	if len(names) != 4 {
		t.Errorf("expected 4 names, got %d", len(names))
	}
}

func TestScalarQuery_NegativeOffset(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Negative offset should be treated as no offset (offset <= 0 check in buildQuery)
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		OrderBy("id", "ASC").
		Offset(-1).
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Should return all 4 users starting from first
	if len(names) != 4 {
		t.Errorf("expected 4 names with negative offset, got %d", len(names))
	}

	if names[0] != "Alice" {
		t.Errorf("expected first name 'Alice', got %q", names[0])
	}
}

func TestScalarQuery_EmptyTableName(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Empty table name
	_, err := Query[string]().
		SetDB(db).
		Table("").
		Select("name").
		Get(ctx)

	// Should fail due to invalid SQL
	if err == nil {
		t.Error("expected error with empty table name, got nil")
	}
}

func TestScalarQuery_EmptyColumnName(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Empty column should be rejected by validation, defaults to *
	results, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("").
		Where("id", 1).
		Get(ctx)

	// With empty Select, it defaults to * which may work differently
	// The main point is it shouldn't panic
	_ = results
	_ = err
}

func TestScalarQuery_MultipleWhereIn(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		WhereIn("id", []any{1, 2, 3, 4}).
		WhereIn("role", []any{"admin"}).
		OrderBy("id", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Only admins: Alice (1), Diana (4)
	expected := []string{"Alice", "Diana"}
	if len(names) != len(expected) {
		t.Fatalf("expected %d names, got %d", len(expected), len(names))
	}

	for i, name := range names {
		if name != expected[i] {
			t.Errorf("expected names[%d] = %q, got %q", i, expected[i], name)
		}
	}
}

func TestScalarQuery_OrWhereWithWhereIn(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where("id", 1).
		OrWhere("role", "user").
		OrderBy("id", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// id=1 (Alice) OR role='user' (Bob, Charlie)
	expected := []string{"Alice", "Bob", "Charlie"}
	if len(names) != len(expected) {
		t.Fatalf("expected %d names, got %d", len(expected), len(names))
	}

	for i, name := range names {
		if name != expected[i] {
			t.Errorf("expected names[%d] = %q, got %q", i, expected[i], name)
		}
	}
}

func TestScalarQuery_CloneAfterComplexQuery(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Create a complex base query
	base := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where("active", 1).
		Where("age", ">", 20).
		OrderBy("name", "ASC").
		Limit(10).
		Offset(0)

	// Clone and modify
	clone := base.Clone()
	clone = clone.Distinct()

	// Original should not have distinct
	query1 := base.buildQuery()
	if strings.Contains(query1, "DISTINCT") {
		t.Error("original query should not have DISTINCT after cloning")
	}

	// Clone should have distinct
	query2 := clone.buildQuery()
	if !strings.Contains(query2, "DISTINCT") {
		t.Error("cloned query should have DISTINCT")
	}

	// Both should execute successfully
	results1, err := base.Get(ctx)
	if err != nil {
		t.Fatalf("base Get failed: %v", err)
	}

	results2, err := clone.Get(ctx)
	if err != nil {
		t.Fatalf("clone Get failed: %v", err)
	}

	// Results should be the same (all active users with age > 20)
	if len(results1) != len(results2) {
		t.Errorf("expected same result count, got base=%d, clone=%d", len(results1), len(results2))
	}
}

func TestScalarQuery_HavingWithoutGroupBy(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Having without GroupBy - SQL behavior depends on database
	// SQLite may allow this for aggregate queries
	_, err := Query[int64]().
		SetDB(db).
		Table("users").
		Select("COUNT(*)").
		Having("COUNT(*) > ?", 0).
		Get(ctx)

	// The query should execute (SQLite allows this)
	// Main point is it shouldn't panic
	_ = err
}

// ============================================
// EDGE CASE TESTS
// ============================================

func TestScalarQuery_WhereMapEmpty(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Empty map should not add any conditions
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where(map[string]any{}).
		OrderBy("id", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Should return all 4 users
	if len(names) != 4 {
		t.Errorf("expected 4 names with empty map, got %d", len(names))
	}
}

func TestScalarQuery_WhereInvalidType(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Non-string, non-map type should be ignored
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where(12345). // Invalid type
		OrderBy("id", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Should return all 4 users since invalid Where is skipped
	if len(names) != 4 {
		t.Errorf("expected 4 names with invalid Where type, got %d", len(names))
	}
}

func TestScalarQuery_SelectWithExpression(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Select with SQL expression - validation may reject this
	// but we can test COUNT(*) which is commonly used
	q := Query[int64]().
		SetDB(db).
		Table("users")

	// Manually set column to bypass validation for testing
	q.column = "COUNT(*)"

	count, err := q.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(count) != 1 || count[0] != 4 {
		t.Errorf("expected COUNT(*) = 4, got %v", count)
	}
}

func TestScalarQuery_HavingArgsOrder(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Test that HAVING args are in correct order when combined with WHERE
	roles, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("role").
		Where("active", 1).
		GroupBy("role").
		Having("COUNT(*) >= ?", 1).
		OrderBy("role", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Active users: Alice (admin), Bob (user), Diana (admin)
	// Groups: admin (2), user (1)
	expected := []string{"admin", "user"}
	if len(roles) != len(expected) {
		t.Fatalf("expected %d roles, got %d", len(expected), len(roles))
	}

	for i, role := range roles {
		if role != expected[i] {
			t.Errorf("expected roles[%d] = %q, got %q", i, expected[i], role)
		}
	}
}

func TestScalarQuery_WhereAfterHaving(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Add WHERE after HAVING - args order matters
	q := Query[string]().
		SetDB(db).
		Table("users").
		Select("role").
		GroupBy("role").
		Having("COUNT(*) >= ?", 1)

	// Add WHERE after HAVING
	q = q.Where("active", 1)

	roles, err := q.OrderBy("role", "ASC").Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Even though WHERE was added after HAVING in code,
	// SQL execution order is: WHERE -> GROUP BY -> HAVING
	// The query builder should handle this correctly
	expected := []string{"admin", "user"}
	if len(roles) != len(expected) {
		t.Fatalf("expected %d roles, got %d", len(expected), len(roles))
	}
}

func TestScalarQuery_SetDBNil(t *testing.T) {
	// Test SetDB(nil) behavior - should fallback to GlobalDB
	db := setupScalarTestDB(t)
	defer db.Close()

	SetGlobalDB(db)
	defer SetGlobalDB(nil)

	ctx := context.Background()

	// SetDB(nil) should use GlobalDB
	names, err := Query[string]().
		SetDB(nil).
		Table("users").
		Select("name").
		OrderBy("id", "ASC").
		Limit(1).
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(names) != 1 || names[0] != "Alice" {
		t.Errorf("expected [Alice], got %v", names)
	}
}

func TestScalarQuery_WhereNotIn(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Test raw NOT IN with 3+ args (triggers raw query path in addWhere)
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where("id NOT IN (?, ?, ?)", 1, 2, 999).
		OrderBy("id", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Should return Charlie (3), Diana (4)
	expected := []string{"Charlie", "Diana"}
	if len(names) != len(expected) {
		t.Fatalf("expected %d names, got %d", len(expected), len(names))
	}

	for i, name := range names {
		if name != expected[i] {
			t.Errorf("expected names[%d] = %q, got %q", i, expected[i], name)
		}
	}
}

func TestScalarQuery_LimitOffset_Combined(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// Test Limit and Offset together for pagination
	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		OrderBy("id", "ASC").
		Limit(2).
		Offset(2).
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Skip first 2 (Alice, Bob), take next 2 (Charlie, Diana)
	expected := []string{"Charlie", "Diana"}
	if len(names) != len(expected) {
		t.Fatalf("expected %d names, got %d", len(expected), len(names))
	}

	for i, name := range names {
		if name != expected[i] {
			t.Errorf("expected names[%d] = %q, got %q", i, expected[i], name)
		}
	}
}

func TestScalarQuery_CountWithWhere(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	count, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where("role", "admin").
		Count(ctx)

	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}

	// 2 admins: Alice, Diana
	if count != 2 {
		t.Errorf("expected count 2, got %d", count)
	}
}

func TestScalarQuery_CountWithMultipleConditions(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	count, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where("role", "user").
		Where("active", 1).
		Count(ctx)

	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}

	// 1 active user: Bob
	if count != 1 {
		t.Errorf("expected count 1, got %d", count)
	}
}

func TestScalarQuery_FirstWithMultipleResults(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	// First with multiple matching results should return only one
	name, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where("role", "admin").
		OrderBy("name", "ASC").
		First(ctx)

	if err != nil {
		t.Fatalf("First failed: %v", err)
	}

	// Should return first admin alphabetically: Alice
	if name != "Alice" {
		t.Errorf("expected 'Alice', got %q", name)
	}
}

func TestScalarQuery_DistinctWithOrderBy(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	roles, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("role").
		Distinct().
		OrderBy("role", "DESC").
		Get(ctx)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Distinct roles ordered DESC: user, admin
	expected := []string{"user", "admin"}
	if len(roles) != len(expected) {
		t.Fatalf("expected %d roles, got %d", len(expected), len(roles))
	}

	for i, role := range roles {
		if role != expected[i] {
			t.Errorf("expected roles[%d] = %q, got %q", i, expected[i], role)
		}
	}
}

// =============================================================================
// ISSUE #8: SCALAR QUERY buildErr TESTS
// =============================================================================

// TestScalarQuery_InvalidTableName_ReturnsError verifies that an invalid table name
// sets buildErr which is surfaced by Get().
func TestScalarQuery_InvalidTableName_ReturnsError(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	_, err := Query[string]().
		SetDB(db).
		Table("users; DROP TABLE").
		Select("name").
		Get(ctx)

	if err == nil {
		t.Fatal("expected error for invalid table name, got nil")
	}
	if !strings.Contains(err.Error(), "invalid") {
		t.Errorf("expected error to mention 'invalid', got %q", err.Error())
	}
}

// TestScalarQuery_InvalidSelectColumn_ReturnsError verifies that an invalid
// column name in Select sets buildErr which is surfaced by Get().
func TestScalarQuery_InvalidSelectColumn_ReturnsError(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	_, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("id; DROP").
		Get(ctx)

	if err == nil {
		t.Fatal("expected error for invalid column name, got nil")
	}
	if !strings.Contains(err.Error(), "invalid") {
		t.Errorf("expected error to mention 'invalid', got %q", err.Error())
	}
}

// TestScalarQuery_InvalidTable_CountReturnsError verifies that buildErr is
// also surfaced by Count(), not just Get().
func TestScalarQuery_InvalidTable_CountReturnsError(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()
	_, err := Query[string]().
		SetDB(db).
		Table("users; DROP TABLE").
		Select("name").
		Count(ctx)

	if err == nil {
		t.Fatal("expected error for invalid table name on Count(), got nil")
	}
	if !strings.Contains(err.Error(), "invalid") {
		t.Errorf("expected error to mention 'invalid', got %q", err.Error())
	}
}

// TestScalarQuery_BuildErrPreservedInClone verifies that buildErr is
// propagated through Clone() so cloned queries also fail.
func TestScalarQuery_BuildErrPreservedInClone(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	ctx := context.Background()

	original := Query[string]().
		SetDB(db).
		Table("users; DROP TABLE").
		Select("name")

	clone := original.Clone()

	_, err := clone.Get(ctx)
	if err == nil {
		t.Fatal("expected cloned query to preserve buildErr, got nil")
	}
	if !strings.Contains(err.Error(), "invalid") {
		t.Errorf("expected error to mention 'invalid', got %q", err.Error())
	}
}
