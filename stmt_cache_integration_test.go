package zorm

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// SCUser is used by the statement-cache routing tests.
type SCUser struct {
	ID   int64
	Name string
}

func (SCUser) TableName() string { return "sc_users" }

// newSCDB creates an in-memory SQLite DB holding a single row whose name marks
// which database it came from.
func newSCDB(t *testing.T, marker string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE sc_users (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO sc_users (id, name) VALUES (1, ?)`, marker); err != nil {
		t.Fatalf("failed to seed: %v", err)
	}
	return db
}

// TestStmtCache_SeparateDBsDoNotShareStatements verifies a prepared statement
// cached while querying one database is never served to a query against a
// different database. Both queries render identical SQL, so a cache keyed only
// by SQL text hands the second query a statement bound to the first DB.
func TestStmtCache_SeparateDBsDoNotShareStatements(t *testing.T) {
	db1 := newSCDB(t, "from-db1")
	defer db1.Close()
	db2 := newSCDB(t, "from-db2")
	defer db2.Close()

	cache := NewStmtCache(10)
	defer cache.Close()

	ctx := context.Background()

	first, err := New[SCUser]().SetDB(db1).WithStmtCache(cache).Where("id", 1).Get(ctx)
	if err != nil {
		t.Fatalf("query against db1 failed: %v", err)
	}
	if len(first) != 1 || first[0].Name != "from-db1" {
		t.Fatalf("expected from-db1, got %+v", first)
	}

	second, err := New[SCUser]().SetDB(db2).WithStmtCache(cache).Where("id", 1).Get(ctx)
	if err != nil {
		t.Fatalf("query against db2 failed: %v", err)
	}
	if len(second) != 1 {
		t.Fatalf("expected 1 row from db2, got %d", len(second))
	}
	if second[0].Name != "from-db2" {
		t.Errorf("query against db2 returned %q: the cached statement is still bound to db1", second[0].Name)
	}
}

// TestStmtCache_ReplicaRotationNotPinned verifies that a shared statement cache
// does not pin every read to whichever replica happened to prepare first.
func TestStmtCache_ReplicaRotationNotPinned(t *testing.T) {
	primary := newSCDB(t, "from-primary")
	defer primary.Close()
	replica1 := newSCDB(t, "from-replica1")
	defer replica1.Close()
	replica2 := newSCDB(t, "from-replica2")
	defer replica2.Close()

	oldDB := GlobalDB
	GlobalDB = nil
	SetGlobalDB(nil)
	ConfigureDBResolver(
		WithPrimary(primary),
		WithReplicas(replica1, replica2),
		WithLoadBalancer(&RoundRobinLoadBalancer{}),
	)
	defer func() {
		ClearDBResolver()
		SetGlobalDB(oldDB)
		GlobalDB = oldDB
	}()

	cache := NewStmtCache(10)
	defer cache.Close()

	ctx := context.Background()

	seen := make(map[string]bool)
	for i := 0; i < 2; i++ {
		rows, err := New[SCUser]().WithStmtCache(cache).Where("id", 1).Get(ctx)
		if err != nil {
			t.Fatalf("read %d failed: %v", i, err)
		}
		if len(rows) != 1 {
			t.Fatalf("read %d returned %d rows, want 1", i, len(rows))
		}
		seen[rows[0].Name] = true
	}

	if len(seen) != 2 {
		t.Errorf("round-robin over 2 replicas returned data from %d replica(s) (%v): the cached statement pins the first one", len(seen), seen)
	}
}

// TestStmtCache_TxStatementNotReusedAfterCommit verifies a statement prepared
// inside a transaction is not stored in the long-lived cache, where it would be
// handed to a later caller after its transaction has already committed.
func TestStmtCache_TxStatementNotReusedAfterCommit(t *testing.T) {
	db := newSCDB(t, "committed")
	defer db.Close()

	oldDB := GlobalDB
	SetGlobalDB(db)
	GlobalDB = db
	defer func() {
		SetGlobalDB(oldDB)
		GlobalDB = oldDB
	}()

	cache := NewStmtCache(10)
	defer cache.Close()

	ctx := context.Background()

	// Prepare and cache the statement from inside a transaction.
	err := Transaction(ctx, func(tx *Tx) error {
		_, err := New[SCUser]().WithTx(tx).WithStmtCache(cache).Where("id", 1).Get(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("in-transaction query failed: %v", err)
	}

	// The transaction is committed; its statement is dead. The same SQL issued
	// outside the transaction must be prepared afresh.
	rows, err := New[SCUser]().WithStmtCache(cache).Where("id", 1).Get(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "statement is closed") || strings.Contains(err.Error(), "transaction has already been committed") {
			t.Fatalf("post-commit query reused the transaction's dead statement: %v", err)
		}
		t.Fatalf("post-commit query failed: %v", err)
	}
	if len(rows) != 1 || rows[0].Name != "committed" {
		t.Errorf("expected the committed row, got %+v", rows)
	}
}
