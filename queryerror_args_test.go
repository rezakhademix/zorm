package zorm

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// The UPDATE paths execute with `allArgs` (CTE args + SET values + WHERE values)
// but reported failures with `values` alone, so the error carried an arg list
// that did not line up with the placeholders in the query it printed — the one
// moment that context matters most.

type QeItem struct {
	ID   int64  `zorm:"primaryKey"`
	Name string `zorm:"column:name"`
}

func (QeItem) TableName() string { return "qe_items" }

func setupQueryErrorDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`
		CREATE TABLE qe_items (
			id   INTEGER PRIMARY KEY,
			name TEXT CHECK (name <> 'rejected')
		);
		INSERT INTO qe_items (id, name) VALUES (1, 'ok');
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

// assertArgsMatchPlaceholders is the invariant under test: an error report is
// only usable if its args line up with the query it prints.
func assertArgsMatchPlaceholders(t *testing.T, err error) {
	t.Helper()

	var qe *QueryError
	if !errors.As(err, &qe) {
		t.Fatalf("expected a *QueryError, got %T: %v", err, err)
	}

	placeholders := strings.Count(qe.Query, "?")
	if placeholders != len(qe.Args) {
		t.Errorf("error reports %d placeholders but %d args\nQuery: %s\nArgs: %v",
			placeholders, len(qe.Args), qe.Query, qe.Args)
	}
}

// TestUpdate_ErrorArgsIncludeCTEArgs covers Update.
func TestUpdate_ErrorArgsIncludeCTEArgs(t *testing.T) {
	db := setupQueryErrorDB(t)
	ctx := context.Background()

	cte := New[QeItem]().SetDB(db).Select("id").Where("id", ">", 0)
	err := New[QeItem]().SetDB(db).
		WithCTE("recent", cte).
		Update(ctx, &QeItem{ID: 1, Name: "rejected"})
	if err == nil {
		t.Fatal("expected the CHECK constraint to reject the update")
	}

	assertArgsMatchPlaceholders(t, err)
}

// TestUpdateColumns_ErrorArgsIncludeCTEArgs covers UpdateColumns.
func TestUpdateColumns_ErrorArgsIncludeCTEArgs(t *testing.T) {
	db := setupQueryErrorDB(t)
	ctx := context.Background()

	cte := New[QeItem]().SetDB(db).Select("id").Where("id", ">", 0)
	err := New[QeItem]().SetDB(db).
		WithCTE("recent", cte).
		UpdateColumns(ctx, &QeItem{ID: 1, Name: "rejected"}, "name")
	if err == nil {
		t.Fatal("expected the CHECK constraint to reject the update")
	}

	assertArgsMatchPlaceholders(t, err)
}

// TestSave_ErrorArgsIncludeCTEArgs covers Save, which needs a tracked entity.
func TestSave_ErrorArgsIncludeCTEArgs(t *testing.T) {
	db := setupQueryErrorDB(t)
	ctx := context.Background()

	row, err := New[QeItem]().SetDB(db).Find(ctx, 1)
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}
	row.Name = "rejected"

	cte := New[QeItem]().SetDB(db).Select("id").Where("id", ">", 0)
	err = New[QeItem]().SetDB(db).WithCTE("recent", cte).Save(ctx, row)
	if err == nil {
		t.Fatal("expected the CHECK constraint to reject the save")
	}

	assertArgsMatchPlaceholders(t, err)
}

// TestDelete_ErrorArgsIncludeCTEArgs covers the same mismatch in execDelete,
// found by auditing the sibling paths after fixing the UPDATE ones.
func TestDelete_ErrorArgsIncludeCTEArgs(t *testing.T) {
	db := setupQueryErrorDB(t)
	ctx := context.Background()

	// A foreign key with no matching parent makes the DELETE fail.
	if _, err := db.Exec(`
		CREATE TABLE qe_children (
			id      INTEGER PRIMARY KEY,
			item_id INTEGER REFERENCES qe_items(id)
		);
		INSERT INTO qe_children (id, item_id) VALUES (1, 1);
		PRAGMA foreign_keys = ON;
	`); err != nil {
		t.Fatalf("failed to add child table: %v", err)
	}

	cte := New[QeItem]().SetDB(db).Select("id").Where("id", ">", 0)
	err := New[QeItem]().SetDB(db).
		WithCTE("recent", cte).
		Where("id", 1).
		Delete(ctx)
	if err == nil {
		t.Fatal("expected the foreign key to reject the delete")
	}

	assertArgsMatchPlaceholders(t, err)
}
