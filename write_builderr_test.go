package zorm

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// The buildErr regime was completed for the read paths (review item #15 / N4)
// and for Delete/DeleteMany, but the write paths that FILTER BY m.wheres were
// missed. A rejected WHERE is dropped from the statement, so an update meant
// for one row runs against the whole table and reports success.

type WbUser struct {
	ID     int64  `zorm:"primaryKey"`
	Name   string `zorm:"column:name"`
	Active bool   `zorm:"column:active"`
}

func (WbUser) TableName() string { return "wb_users" }

func setupWriteBuildErrDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`
		CREATE TABLE wb_users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER);
		INSERT INTO wb_users (id, name, active) VALUES (1,'a',1),(2,'b',1),(3,'c',1);
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

func activeCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM wb_users WHERE active = 1`).Scan(&n); err != nil {
		t.Fatalf("count failed: %v", err)
	}
	return n
}

// TestUpdateMany_SurfacesBuildErr is the mass-update regression: the rejected
// operator drops the WHERE, and without a guard every row is updated.
func TestUpdateMany_SurfacesBuildErr(t *testing.T) {
	db := setupWriteBuildErrDB(t)
	ctx := context.Background()

	m := New[WbUser]().SetDB(db).Where("name", "= 'a' OR 1=1 --", "a")
	if m.buildErr == nil {
		t.Fatal("precondition: expected the invalid operator to set buildErr")
	}

	err := m.UpdateMany(ctx, map[string]any{"active": false})
	if err == nil {
		t.Error("expected UpdateMany to refuse to run with a build error")
	}
	if n := activeCount(t, db); n != 3 {
		t.Errorf("expected all 3 rows untouched, only %d still active", n)
	}
}

// TestUpdateManyByKey_SurfacesBuildErr covers the chunked sibling.
func TestUpdateManyByKey_SurfacesBuildErr(t *testing.T) {
	db := setupWriteBuildErrDB(t)
	ctx := context.Background()

	m := New[WbUser]().SetDB(db).Where("name", "BOGUS_OP", "a")
	if m.buildErr == nil {
		t.Fatal("precondition: expected buildErr")
	}

	err := m.UpdateManyByKey(ctx, "id", "name", map[any]any{int64(1): "changed"})
	if err == nil {
		t.Error("expected UpdateManyByKey to refuse to run with a build error")
	}

	var name string
	if err := db.QueryRow(`SELECT name FROM wb_users WHERE id = 1`).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "a" {
		t.Errorf("expected row 1 untouched, got %q", name)
	}
}

// TestWritePaths_SurfaceBuildErr covers the remaining write terminals. They
// filter by primary key rather than m.wheres, but a rejected WithCTE still
// silently changes the statement they emit, so they belong to the same regime.
func TestWritePaths_SurfaceBuildErr(t *testing.T) {
	db := setupWriteBuildErrDB(t)
	ctx := context.Background()

	broken := func() *Model[WbUser] {
		return New[WbUser]().SetDB(db).WithCTE("bad name; DROP TABLE wb_users", "SELECT 1")
	}

	cases := []struct {
		name string
		run  func() error
	}{
		{"Create", func() error { return broken().Create(ctx, &WbUser{Name: "x"}) }},
		{"Update", func() error { return broken().Update(ctx, &WbUser{ID: 1, Name: "x"}) }},
		{"UpdateColumns", func() error { return broken().UpdateColumns(ctx, &WbUser{ID: 1, Name: "x"}, "name") }},
		{"CreateMany", func() error { return broken().CreateMany(ctx, []*WbUser{{Name: "x"}}) }},
		{"BulkInsert", func() error { return broken().BulkInsert(ctx, []*WbUser{{Name: "x"}}) }},
		{"UpdateMany", func() error { return broken().UpdateMany(ctx, map[string]any{"name": "x"}) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, ErrInvalidColumnName) {
				t.Errorf("expected ErrInvalidColumnName, got %v", err)
			}
		})
	}
}

// TestSave_SurfacesBuildErr needs a tracked entity, so it stands alone.
func TestSave_SurfacesBuildErr(t *testing.T) {
	db := setupWriteBuildErrDB(t)
	ctx := context.Background()

	row, err := New[WbUser]().SetDB(db).Find(ctx, 1)
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}
	row.Name = "changed"

	err = New[WbUser]().SetDB(db).
		WithCTE("bad name; DROP TABLE wb_users", "SELECT 1").
		Save(ctx, row)
	if !errors.Is(err, ErrInvalidColumnName) {
		t.Errorf("expected ErrInvalidColumnName, got %v", err)
	}
}

// TestUpdateMany_ValidQueryUnaffected guards against overcorrecting.
func TestUpdateMany_ValidQueryUnaffected(t *testing.T) {
	db := setupWriteBuildErrDB(t)
	ctx := context.Background()

	if err := New[WbUser]().SetDB(db).Where("id", 1).UpdateMany(ctx, map[string]any{"active": false}); err != nil {
		t.Fatalf("UpdateMany failed: %v", err)
	}
	if n := activeCount(t, db); n != 2 {
		t.Errorf("expected exactly 1 row deactivated, %d still active", n)
	}
}
