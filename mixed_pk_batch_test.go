package zorm

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// insertColumns decides ONE column list for the whole batch, including the auto
// primary key when any entity has it set. That cannot serve a batch mixing set
// and unset PKs: the zero-PK entities then bind a literal 0 instead of being
// auto-assigned. The first such row lands as id=0 and the next collides with it.

type MpItem struct {
	ID   int64  `zorm:"primaryKey"`
	Name string `zorm:"column:name"`
}

func (MpItem) TableName() string { return "mp_items" }

func setupMixedPKDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(`CREATE TABLE mp_items (id INTEGER PRIMARY KEY, name TEXT);`); err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

func mpIDByName(t *testing.T, db *sql.DB, name string) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(`SELECT id FROM mp_items WHERE name = ?`, name).Scan(&id); err != nil {
		t.Fatalf("row %q not found: %v", name, err)
	}
	return id
}

// assertMixedBatch is the shared expectation: explicit PKs are honored, and
// every zero-PK entity gets a real auto-assigned id (never 0, never colliding).
func assertMixedBatch(t *testing.T, db *sql.DB, rows []*MpItem) {
	t.Helper()

	if got := mpIDByName(t, db, "explicit"); got != 77 {
		t.Errorf("expected the explicit primary key 77, got %d", got)
	}
	for _, name := range []string{"zero", "zero2"} {
		if got := mpIDByName(t, db, name); got == 0 {
			t.Errorf("row %q was written with a literal id=0", name)
		}
	}

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM mp_items`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("expected all 3 rows inserted, got %d", n)
	}

	// The entities must carry back the ids that were actually written.
	for _, r := range rows {
		if r.ID == 0 {
			t.Errorf("entity %q was not assigned its primary key", r.Name)
		}
	}
}

// TestBulkInsert_MixedPKBatch covers the per-row prepared-statement path.
func TestBulkInsert_MixedPKBatch(t *testing.T) {
	db := setupMixedPKDB(t)

	rows := []*MpItem{{Name: "zero"}, {ID: 77, Name: "explicit"}, {Name: "zero2"}}
	if err := New[MpItem]().SetDB(db).BulkInsert(context.Background(), rows); err != nil {
		t.Fatalf("BulkInsert failed: %v", err)
	}
	assertMixedBatch(t, db, rows)
}

// TestCreateMany_MixedPKBatch covers the multi-row VALUES path.
func TestCreateMany_MixedPKBatch(t *testing.T) {
	db := setupMixedPKDB(t)

	rows := []*MpItem{{Name: "zero"}, {ID: 77, Name: "explicit"}, {Name: "zero2"}}
	if err := New[MpItem]().SetDB(db).CreateMany(context.Background(), rows); err != nil {
		t.Fatalf("CreateMany failed: %v", err)
	}
	assertMixedBatch(t, db, rows)
}

// TestCreateMany_AllExplicitPKsUnaffected and its sibling guard the two
// homogeneous batches, which always worked.
func TestCreateMany_AllExplicitPKsUnaffected(t *testing.T) {
	db := setupMixedPKDB(t)

	rows := []*MpItem{{ID: 10, Name: "a"}, {ID: 11, Name: "b"}}
	if err := New[MpItem]().SetDB(db).CreateMany(context.Background(), rows); err != nil {
		t.Fatalf("CreateMany failed: %v", err)
	}
	if got := mpIDByName(t, db, "a"); got != 10 {
		t.Errorf("expected id 10, got %d", got)
	}
	if got := mpIDByName(t, db, "b"); got != 11 {
		t.Errorf("expected id 11, got %d", got)
	}
}

func TestCreateMany_AllAutoPKsUnaffected(t *testing.T) {
	db := setupMixedPKDB(t)

	rows := []*MpItem{{Name: "a"}, {Name: "b"}}
	if err := New[MpItem]().SetDB(db).CreateMany(context.Background(), rows); err != nil {
		t.Fatalf("CreateMany failed: %v", err)
	}
	for _, r := range rows {
		if r.ID == 0 {
			t.Errorf("entity %q was not assigned a primary key", r.Name)
		}
	}
}
