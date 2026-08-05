package zorm

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// BiItem exercises BulkInsert's hook wiring: BeforeCreate must run (and its
// mutations must reach the INSERT), created_at must be auto-set, and the PK
// column decision must consider every entity in the batch.
type BiItem struct {
	ID        int       `zorm:"primaryKey"`
	Name      string    `zorm:"column:name"`
	Value     int       `zorm:"column:value"`
	CreatedAt time.Time `zorm:"column:created_at"`

	BeforeCalled int `zorm:"-"`
}

func (BiItem) TableName() string { return "bi_items" }

func (b *BiItem) BeforeCreate(ctx context.Context) error {
	b.BeforeCalled++
	b.Value += 100
	return nil
}

// BiFailItem aborts the batch from BeforeCreate on a named row.
type BiFailItem struct {
	ID   int    `zorm:"primaryKey"`
	Name string `zorm:"column:name"`
}

func (BiFailItem) TableName() string { return "bi_items" }

var errBiBeforeCreate = errors.New("before-create rejected")

func (b *BiFailItem) BeforeCreate(ctx context.Context) error {
	if b.Name == "bad" {
		return errBiBeforeCreate
	}
	return nil
}

// BiPlainItem has no hooks; used for the mixed-PK batch.
type BiPlainItem struct {
	ID    int    `zorm:"primaryKey"`
	Name  string `zorm:"column:name"`
	Value int    `zorm:"column:value"`
}

func (BiPlainItem) TableName() string { return "bi_items" }

func setupBulkInsertDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`
		CREATE TABLE bi_items (
			id         INTEGER PRIMARY KEY,
			name       TEXT,
			value      INTEGER,
			created_at DATETIME
		);
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

// TestBulkInsert_CallsBeforeCreate is the headline of README's "use BulkInsert
// if you need hooks": BulkInsert only fired AfterCreate, so BeforeCreate never
// ran and its mutations never reached the INSERT.
func TestBulkInsert_CallsBeforeCreate(t *testing.T) {
	db := setupBulkInsertDB(t)
	ctx := context.Background()

	items := []*BiItem{{Name: "a", Value: 1}, {Name: "b", Value: 2}}
	if err := New[BiItem]().SetDB(db).BulkInsert(ctx, items); err != nil {
		t.Fatalf("BulkInsert failed: %v", err)
	}

	for i, item := range items {
		if item.BeforeCalled != 1 {
			t.Errorf("item[%d]: expected BeforeCreate called once, got %d", i, item.BeforeCalled)
		}
	}

	var stored int
	if err := db.QueryRow("SELECT value FROM bi_items WHERE name = 'a'").Scan(&stored); err != nil {
		t.Fatalf("failed to read back row: %v", err)
	}
	if stored != 101 {
		t.Errorf("expected BeforeCreate's mutation (101) to be inserted, got %d", stored)
	}
}

// TestBulkInsert_BeforeCreateErrorAborts verifies a hook error stops the batch
// and leaves nothing behind.
func TestBulkInsert_BeforeCreateErrorAborts(t *testing.T) {
	db := setupBulkInsertDB(t)
	ctx := context.Background()

	items := []*BiFailItem{{Name: "ok"}, {Name: "bad"}}
	err := New[BiFailItem]().SetDB(db).BulkInsert(ctx, items)
	if !errors.Is(err, errBiBeforeCreate) {
		t.Fatalf("expected the BeforeCreate error back, got %v", err)
	}

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM bi_items").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("expected no rows after an aborted batch, got %d", count)
	}
}

// TestBulkInsert_SetsCreatedAt verifies BulkInsert applies the same zero-only
// created_at rule as Create and CreateMany.
func TestBulkInsert_SetsCreatedAt(t *testing.T) {
	db := setupBulkInsertDB(t)
	ctx := context.Background()

	explicit := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	items := []*BiItem{{Name: "auto"}, {Name: "explicit", CreatedAt: explicit}}
	if err := New[BiItem]().SetDB(db).BulkInsert(ctx, items); err != nil {
		t.Fatalf("BulkInsert failed: %v", err)
	}

	if items[0].CreatedAt.IsZero() {
		t.Error("expected created_at to be auto-set on the entity")
	}
	if !items[1].CreatedAt.Equal(explicit) {
		t.Errorf("expected the explicit created_at to be preserved, got %v", items[1].CreatedAt)
	}

	var stored time.Time
	if err := db.QueryRow("SELECT created_at FROM bi_items WHERE name = 'auto'").Scan(&stored); err != nil {
		t.Fatalf("failed to read back created_at: %v", err)
	}
	if stored.IsZero() {
		t.Error("expected the auto-set created_at to be inserted")
	}
}

// TestBulkInsert_MixedPrimaryKeys covers the columns-from-entities[0] bug: the
// first entity's zero PK dropped the id column for the whole batch, so the
// later entity's explicit PK was silently discarded.
func TestBulkInsert_MixedPrimaryKeys(t *testing.T) {
	db := setupBulkInsertDB(t)
	ctx := context.Background()

	items := []*BiPlainItem{{Name: "auto"}, {ID: 77, Name: "explicit"}}
	if err := New[BiPlainItem]().SetDB(db).BulkInsert(ctx, items); err != nil {
		t.Fatalf("BulkInsert failed: %v", err)
	}

	var id int
	if err := db.QueryRow("SELECT id FROM bi_items WHERE name = 'explicit'").Scan(&id); err != nil {
		t.Fatalf("failed to read back row: %v", err)
	}
	if id != 77 {
		t.Errorf("expected the explicit primary key 77 to be honored, got %d", id)
	}

	// The zero-PK half must be auto-assigned, not written as a literal 0.
	// Asserting only the explicit id let a mixed batch that inserted id=0 pass.
	if err := db.QueryRow("SELECT id FROM bi_items WHERE name = 'auto'").Scan(&id); err != nil {
		t.Fatalf("failed to read back the auto row: %v", err)
	}
	if id == 0 {
		t.Error("the zero-PK row was written with a literal id=0")
	}
	if items[0].ID == 0 {
		t.Error("the zero-PK entity was not assigned its primary key")
	}
}
