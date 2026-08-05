package zorm

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// ShItem counts update-hook calls so a clean Save can be distinguished from a
// dirty one.
type ShItem struct {
	ID    int64  `zorm:"primaryKey"`
	Name  string `zorm:"column:name"`
	Value int    `zorm:"column:value"`

	BeforeCount int `zorm:"-"`
	AfterCount  int `zorm:"-"`
}

func (ShItem) TableName() string { return "sh_items" }

func (s *ShItem) BeforeUpdate(ctx context.Context) error {
	s.BeforeCount++
	return nil
}

func (s *ShItem) AfterUpdate(ctx context.Context) error {
	s.AfterCount++
	return nil
}

func setupSaveHookDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`
		CREATE TABLE sh_items (id INTEGER PRIMARY KEY, name TEXT, value INTEGER);
		INSERT INTO sh_items (id, name, value) VALUES (1, 'A', 1);
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

// TestSave_CleanEntityStillFiresUpdateHooks pins the contract the Save doc
// comment states and the README used to contradict: a Save with nothing dirty
// issues no SQL but still fires BeforeUpdate and AfterUpdate, so audit hooks
// observe every Save call.
func TestSave_CleanEntityStillFiresUpdateHooks(t *testing.T) {
	db := setupSaveHookDB(t)
	ctx := context.Background()

	m := New[ShItem]().SetDB(db)
	row, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	// Out-of-band marker: if Save issues an UPDATE it will overwrite this.
	if _, err := db.Exec(`UPDATE sh_items SET value = 999 WHERE id = 1`); err != nil {
		t.Fatalf("marker update failed: %v", err)
	}

	if err := m.Save(ctx, row); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	if row.BeforeCount != 1 {
		t.Errorf("expected BeforeUpdate to fire once on a clean Save, got %d", row.BeforeCount)
	}
	if row.AfterCount != 1 {
		t.Errorf("expected AfterUpdate to fire once on a clean Save, got %d", row.AfterCount)
	}

	var value int
	if err := db.QueryRow(`SELECT value FROM sh_items WHERE id = 1`).Scan(&value); err != nil {
		t.Fatalf("failed to read back row: %v", err)
	}
	if value != 999 {
		t.Errorf("expected a clean Save to issue no SQL (marker 999), got %d", value)
	}
}

// TestSave_DirtyEntityFiresUpdateHooksOnce is the companion case: the hooks
// fire in the same positions when SQL is issued.
func TestSave_DirtyEntityFiresUpdateHooksOnce(t *testing.T) {
	db := setupSaveHookDB(t)
	ctx := context.Background()

	m := New[ShItem]().SetDB(db)
	row, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	row.Name = "B"
	if err := m.Save(ctx, row); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	if row.BeforeCount != 1 || row.AfterCount != 1 {
		t.Errorf("expected each hook once, got before=%d after=%d", row.BeforeCount, row.AfterCount)
	}

	var name string
	if err := db.QueryRow(`SELECT name FROM sh_items WHERE id = 1`).Scan(&name); err != nil {
		t.Fatalf("failed to read back row: %v", err)
	}
	if name != "B" {
		t.Errorf("expected the dirty column to be written, got %q", name)
	}
}
