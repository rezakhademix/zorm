package zorm

import (
	"context"
	"database/sql"
	"sort"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// SyncUser drives the Sync atomicity tests. Its pivot table carries a CHECK
// constraint so an attach can be made to fail after the detach has already run.
type SyncUser struct {
	ID    int    `zorm:"primaryKey"`
	Name  string `zorm:"column:name"`
	Roles []*SyncRole
}

func (SyncUser) TableName() string { return "sync_users" }

func (SyncUser) RolesRelation() BelongsToMany[SyncRole] {
	return BelongsToMany[SyncRole]{
		PivotTable: "sync_role_user",
		ForeignKey: "user_id",
		RelatedKey: "role_id",
	}
}

// SyncRole is the related side of the many-to-many under test.
type SyncRole struct {
	ID   int    `zorm:"primaryKey"`
	Name string `zorm:"column:name"`
}

func (SyncRole) TableName() string { return "sync_roles" }

func setupSyncDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	// A ":memory:" database is per-connection, so pin the pool to one connection.
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`
		CREATE TABLE sync_users (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE sync_roles (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE sync_role_user (
			user_id INTEGER,
			role_id INTEGER CHECK (role_id < 100)
		);

		INSERT INTO sync_users (id, name) VALUES (1, 'Alice');
		INSERT INTO sync_roles (id, name) VALUES (1, 'Admin'), (2, 'Editor'), (3, 'Viewer');
		INSERT INTO sync_role_user (user_id, role_id) VALUES (1, 1), (1, 2);
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

func syncPivotIDs(t *testing.T, db *sql.DB, userID int) []int {
	t.Helper()

	rows, err := db.Query("SELECT role_id FROM sync_role_user WHERE user_id = ? ORDER BY role_id", userID)
	if err != nil {
		t.Fatalf("failed to read pivot rows: %v", err)
	}
	defer rows.Close()

	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("failed to scan pivot row: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("failed to iterate pivot rows: %v", err)
	}
	sort.Ints(ids)
	return ids
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestSync_RollsBackDetachWhenAttachFails is the atomicity regression: Sync
// detaches then attaches, so a failing attach used to leave the association
// half-synced with the detached rows already gone.
func TestSync_RollsBackDetachWhenAttachFails(t *testing.T) {
	db := setupSyncDB(t)
	ctx := context.Background()

	user := &SyncUser{ID: 1}

	// Current: {1, 2}. Target: {1, 999} -> detach 2, attach 999.
	// 999 violates the pivot CHECK, so the attach fails after the detach ran.
	err := New[SyncUser]().SetDB(db).Sync(ctx, user, "Roles", []any{1, 999}, nil)
	if err == nil {
		t.Fatal("expected Sync to fail on the CHECK constraint")
	}

	got := syncPivotIDs(t, db, 1)
	if !equalInts(got, []int{1, 2}) {
		t.Errorf("expected pivot state to be rolled back to [1 2], got %v", got)
	}
}

// TestSync_SucceedsAtomically guards against overcorrecting: a valid Sync must
// still apply both halves.
func TestSync_SucceedsAtomically(t *testing.T) {
	db := setupSyncDB(t)
	ctx := context.Background()

	user := &SyncUser{ID: 1}

	if err := New[SyncUser]().SetDB(db).Sync(ctx, user, "Roles", []any{1, 3}, nil); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	got := syncPivotIDs(t, db, 1)
	if !equalInts(got, []int{1, 3}) {
		t.Errorf("expected pivot state [1 3], got %v", got)
	}
}

// TestSync_InsideCallerTransactionParticipates verifies Sync joins an existing
// transaction instead of opening a nested one, and that rolling that
// transaction back undoes the Sync.
func TestSync_InsideCallerTransactionParticipates(t *testing.T) {
	db := setupSyncDB(t)
	ctx := context.Background()

	user := &SyncUser{ID: 1}
	sentinel := context.Canceled

	err := New[SyncUser]().SetDB(db).Transaction(ctx, func(tx *Tx) error {
		if err := New[SyncUser]().SetDB(db).WithTx(tx).Sync(ctx, user, "Roles", []any{3}, nil); err != nil {
			return err
		}
		return sentinel
	})
	if err != sentinel {
		t.Fatalf("expected the sentinel error back from Transaction, got %v", err)
	}

	got := syncPivotIDs(t, db, 1)
	if !equalInts(got, []int{1, 2}) {
		t.Errorf("expected the caller's rollback to undo Sync, got %v", got)
	}
}
