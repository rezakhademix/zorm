package zorm

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// TOUser is written to two structurally identical tables: its own ("to_users")
// and an archive table reached only via Table("to_archived_users").
type TOUser struct {
	ID        int64
	Name      string
	UpdatedAt time.Time
}

func (TOUser) TableName() string { return "to_users" }

const toArchiveTable = "to_archived_users"

// setupTableOverrideDB creates two identical tables so a write aimed at the
// archive table can be told apart from one that leaked into the default table.
func setupTableOverrideDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE to_users (
			id         INTEGER PRIMARY KEY,
			name       TEXT,
			updated_at DATETIME
		);
		CREATE TABLE to_archived_users (
			id         INTEGER PRIMARY KEY,
			name       TEXT,
			updated_at DATETIME
		);

		INSERT INTO to_users          (id, name, updated_at) VALUES (1, 'live',     '2025-01-01 00:00:00');
		INSERT INTO to_archived_users (id, name, updated_at) VALUES (1, 'archived', '2025-01-01 00:00:00');
	`)
	if err != nil {
		t.Fatalf("failed to setup table-override DB: %v", err)
	}
	return db
}

// countRows returns the number of rows in table matching the name column.
func countRows(t *testing.T, db *sql.DB, table, name string) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE name = ?", name).Scan(&n); err != nil {
		t.Fatalf("count on %s failed: %v", table, err)
	}
	return n
}

// nameByID returns the name column of the row with the given id.
func nameByID(t *testing.T, db *sql.DB, table string, id int64) string {
	t.Helper()
	var name string
	if err := db.QueryRow("SELECT name FROM "+table+" WHERE id = ?", id).Scan(&name); err != nil {
		t.Fatalf("select on %s failed: %v", table, err)
	}
	return name
}

func TestTableOverride_Create(t *testing.T) {
	db := setupTableOverrideDB(t)
	defer db.Close()

	ctx := context.Background()
	user := &TOUser{Name: "inserted"}

	if err := New[TOUser]().SetDB(db).Table(toArchiveTable).Create(ctx, user); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if got := countRows(t, db, toArchiveTable, "inserted"); got != 1 {
		t.Errorf("expected the row in %s, found %d", toArchiveTable, got)
	}
	if got := countRows(t, db, "to_users", "inserted"); got != 0 {
		t.Errorf("row leaked into to_users: found %d", got)
	}
}

func TestTableOverride_Update(t *testing.T) {
	db := setupTableOverrideDB(t)
	defer db.Close()

	ctx := context.Background()
	user := &TOUser{ID: 1, Name: "updated"}

	if err := New[TOUser]().SetDB(db).Table(toArchiveTable).Update(ctx, user); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if got := nameByID(t, db, toArchiveTable, 1); got != "updated" {
		t.Errorf("expected %s row updated, got name %q", toArchiveTable, got)
	}
	if got := nameByID(t, db, "to_users", 1); got != "live" {
		t.Errorf("to_users row was modified: name is %q, want %q", got, "live")
	}
}

func TestTableOverride_UpdateColumns(t *testing.T) {
	db := setupTableOverrideDB(t)
	defer db.Close()

	ctx := context.Background()
	user := &TOUser{ID: 1, Name: "patched"}

	if err := New[TOUser]().SetDB(db).Table(toArchiveTable).UpdateColumns(ctx, user, "name"); err != nil {
		t.Fatalf("UpdateColumns failed: %v", err)
	}

	if got := nameByID(t, db, toArchiveTable, 1); got != "patched" {
		t.Errorf("expected %s row patched, got name %q", toArchiveTable, got)
	}
	if got := nameByID(t, db, "to_users", 1); got != "live" {
		t.Errorf("to_users row was modified: name is %q, want %q", got, "live")
	}
}

func TestTableOverride_Save(t *testing.T) {
	db := setupTableOverrideDB(t)
	defer db.Close()

	ctx := context.Background()

	// Load through the override so the entity carries a dirty-tracking baseline
	// taken from the archive table.
	user, err := New[TOUser]().SetDB(db).Table(toArchiveTable).Find(ctx, 1)
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}
	if user.Name != "archived" {
		t.Fatalf("expected to load the archived row, got name %q", user.Name)
	}

	user.Name = "saved"
	if err := New[TOUser]().SetDB(db).Table(toArchiveTable).Save(ctx, user); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	if got := nameByID(t, db, toArchiveTable, 1); got != "saved" {
		t.Errorf("expected %s row saved, got name %q", toArchiveTable, got)
	}
	if got := nameByID(t, db, "to_users", 1); got != "live" {
		t.Errorf("to_users row was modified: name is %q, want %q", got, "live")
	}
}

func TestTableOverride_Delete(t *testing.T) {
	db := setupTableOverrideDB(t)
	defer db.Close()

	ctx := context.Background()

	if err := New[TOUser]().SetDB(db).Table(toArchiveTable).Where("id", 1).Delete(ctx); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if got := countRows(t, db, toArchiveTable, "archived"); got != 0 {
		t.Errorf("expected the %s row deleted, %d remain", toArchiveTable, got)
	}
	if got := countRows(t, db, "to_users", "live"); got != 1 {
		t.Errorf("to_users row was deleted: %d remain, want 1", got)
	}
}
