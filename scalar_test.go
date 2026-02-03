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
