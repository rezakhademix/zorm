package zorm

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type saveItem struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (saveItem) TableName() string { return "save_items" }

type saveVersionedItem struct {
	ID      int `zorm:"primaryKey"`
	Name    string
	Value   int
	Version int64 `zorm:"version"`
}

func (saveVersionedItem) TableName() string { return "save_versioned_items" }

type saveItemWithTouch struct {
	ID        int `zorm:"primaryKey"`
	Name      string
	UpdatedAt time.Time
}

func (saveItemWithTouch) TableName() string { return "save_items_touch" }

func setupSaveDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	_, err = db.Exec(`
		CREATE TABLE save_items (
			id    INTEGER PRIMARY KEY,
			name  TEXT,
			value INTEGER
		);
		INSERT INTO save_items (name, value) VALUES ('A', 10), ('B', 20);

		CREATE TABLE save_versioned_items (
			id      INTEGER PRIMARY KEY,
			name    TEXT,
			value   INTEGER,
			version INTEGER NOT NULL DEFAULT 1
		);
		INSERT INTO save_versioned_items (name, value, version) VALUES ('A', 10, 1), ('B', 20, 1);

		CREATE TABLE save_items_touch (
			id         INTEGER PRIMARY KEY,
			name       TEXT,
			updated_at DATETIME
		);
		INSERT INTO save_items_touch (name, updated_at) VALUES ('A', '2000-01-01 00:00:00');
	`)
	if err != nil {
		t.Fatalf("setup db: %v", err)
	}
	return db
}

func TestSave_NoOp_WhenNothingDirty(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()

	ctx := context.Background()
	m := New[saveItem]().SetDB(db)

	row, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}

	// Out-of-band marker so we can prove Save did not execute an UPDATE.
	if _, err := db.Exec(`UPDATE save_items SET value = 999 WHERE id = 1`); err != nil {
		t.Fatalf("marker update: %v", err)
	}

	if err := m.Save(ctx, row); err != nil {
		t.Fatalf("Save: %v", err)
	}

	fresh, err := New[saveItem]().SetDB(db).Find(ctx, 1)
	if err != nil {
		t.Fatalf("re-Find: %v", err)
	}
	if fresh.Value != 999 {
		t.Errorf("expected marker value 999 (Save should be a no-op), got %d", fresh.Value)
	}
}

func TestSave_OnlyDirtyColumns(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()

	ctx := context.Background()
	m := New[saveItem]().SetDB(db)

	row, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}

	// Marker on value: if Save writes value back, this gets clobbered.
	if _, err := db.Exec(`UPDATE save_items SET value = 999 WHERE id = 1`); err != nil {
		t.Fatalf("marker update: %v", err)
	}

	row.Name = "Renamed"
	if err := m.Save(ctx, row); err != nil {
		t.Fatalf("Save: %v", err)
	}

	fresh, err := New[saveItem]().SetDB(db).Find(ctx, 1)
	if err != nil {
		t.Fatalf("re-Find: %v", err)
	}
	if fresh.Name != "Renamed" {
		t.Errorf("expected name 'Renamed', got %q", fresh.Name)
	}
	if fresh.Value != 999 {
		t.Errorf("expected marker value 999 preserved (only dirty cols), got %d", fresh.Value)
	}
}

func TestSave_VersionIncrement(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()

	ctx := context.Background()
	m := New[saveVersionedItem]().SetDB(db)

	row, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if row.Version != 1 {
		t.Fatalf("expected loaded version 1, got %d", row.Version)
	}

	row.Name = "Updated"
	if err := m.Save(ctx, row); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if row.Version != 2 {
		t.Errorf("expected in-memory version 2 after Save, got %d", row.Version)
	}

	var dbVersion int64
	if err := db.QueryRow(`SELECT version FROM save_versioned_items WHERE id = 1`).Scan(&dbVersion); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if dbVersion != 2 {
		t.Errorf("expected db version 2, got %d", dbVersion)
	}
}

func TestSave_OptimisticLockConflict(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()

	ctx := context.Background()
	mA := New[saveVersionedItem]().SetDB(db)
	mB := New[saveVersionedItem]().SetDB(db)

	a, err := mA.Find(ctx, 1)
	if err != nil {
		t.Fatalf("Find A: %v", err)
	}
	b, err := mB.Find(ctx, 1)
	if err != nil {
		t.Fatalf("Find B: %v", err)
	}

	a.Name = "from-A"
	if err := mA.Save(ctx, a); err != nil {
		t.Fatalf("Save A: %v", err)
	}

	b.Name = "from-B"
	err = mB.Save(ctx, b)
	if err == nil {
		t.Fatal("expected conflict, got nil")
	}
	if !IsOptimisticLock(err) {
		t.Errorf("expected ErrOptimisticLock, got %v", err)
	}
	if !errors.Is(err, ErrOptimisticLock) {
		t.Errorf("errors.Is(err, ErrOptimisticLock) = false")
	}
}

func TestSave_RequiresPK(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()

	ctx := context.Background()
	row := &saveItem{Name: "no-pk", Value: 1}

	err := New[saveItem]().SetDB(db).Save(ctx, row)
	if err == nil {
		t.Fatal("expected error for zero PK, got nil")
	}
}

func TestSave_RejectsUntrackedEntity(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()

	ctx := context.Background()

	// Out-of-band: ensure target row really exists with known values.
	if _, err := db.Exec(`UPDATE save_items SET name = 'original', value = 42 WHERE id = 1`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Manually-constructed entity (not loaded via Find/First/Get => untracked).
	// Caller only meant to change Name, but Value is left at the zero value.
	// Without the untracked-rejection guard, Save would silently rewrite Value
	// to 0 because getDirty marks every non-PK field as dirty for untracked
	// entities.
	row := &saveItem{ID: 1, Name: "intended"}

	err := New[saveItem]().SetDB(db).Save(ctx, row)
	if !errors.Is(err, ErrSaveUntracked) {
		t.Fatalf("expected ErrSaveUntracked, got %v", err)
	}

	var name string
	var value int
	if err := db.QueryRow(`SELECT name, value FROM save_items WHERE id = 1`).Scan(&name, &value); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if name != "original" || value != 42 {
		t.Errorf("row mutated by rejected Save: name=%q value=%d (want 'original'/42)", name, value)
	}
}

// saveHookEntity tracks BeforeUpdate / AfterUpdate invocation counts.
type saveHookEntity struct {
	ID       int `zorm:"primaryKey"`
	Name     string
	Value    int
	beforeCt int `zorm:"-"`
	afterCt  int `zorm:"-"`
}

func (saveHookEntity) TableName() string { return "save_items" }

func (h *saveHookEntity) BeforeUpdate(_ context.Context) error {
	h.beforeCt++
	return nil
}

func (h *saveHookEntity) AfterUpdate(_ context.Context) error {
	h.afterCt++
	return nil
}

func TestSave_FiresHooks(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()

	ctx := context.Background()
	m := New[saveHookEntity]().SetDB(db)

	row, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	row.Name = "X"
	if err := m.Save(ctx, row); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if row.beforeCt != 1 {
		t.Errorf("BeforeUpdate count = %d, want 1", row.beforeCt)
	}
	if row.afterCt != 1 {
		t.Errorf("AfterUpdate count = %d, want 1", row.afterCt)
	}
}

func TestSave_WithTx_Rollback(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()

	ctx := context.Background()
	m := New[saveItem]().SetDB(db)

	row, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}

	sentinel := errors.New("rollback me")
	err = m.Transaction(ctx, func(tx *Tx) error {
		row.Name = "TxRenamed"
		if err := m.WithTx(tx).Save(ctx, row); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}

	fresh, err := New[saveItem]().SetDB(db).Find(ctx, 1)
	if err != nil {
		t.Fatalf("re-Find: %v", err)
	}
	if fresh.Name == "TxRenamed" {
		t.Errorf("expected rollback to undo Save; row name still %q", fresh.Name)
	}
}

func TestSave_RowDeletedConcurrently(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()

	ctx := context.Background()
	m := New[saveItem]().SetDB(db)

	row, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM save_items WHERE id = 1`); err != nil {
		t.Fatalf("concurrent delete: %v", err)
	}

	row.Name = "ghost"
	err = m.Save(ctx, row)
	if err == nil {
		t.Fatal("expected ErrRecordNotFound, got nil")
	}
	if !errors.Is(err, ErrRecordNotFound) {
		t.Errorf("expected ErrRecordNotFound, got %v", err)
	}
}

func TestSave_TouchesUpdatedAt_WhenColumnExists(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()

	ctx := context.Background()
	m := New[saveItemWithTouch]().SetDB(db)

	row, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	before := row.UpdatedAt

	row.Name = "Touched"
	if err := m.Save(ctx, row); err != nil {
		t.Fatalf("Save: %v", err)
	}

	fresh, err := New[saveItemWithTouch]().SetDB(db).Find(ctx, 1)
	if err != nil {
		t.Fatalf("re-Find: %v", err)
	}
	if !fresh.UpdatedAt.After(before) {
		t.Errorf("expected updated_at to advance past %v, got %v", before, fresh.UpdatedAt)
	}
}

// Parse-time validation tests (no DB needed; ParseModel panics on bad input).

type badVersionType struct {
	ID      int    `zorm:"primaryKey"`
	Version string `zorm:"version"`
}

func (badVersionType) TableName() string { return "bad_version_string" }

type doubleVersion struct {
	ID  int   `zorm:"primaryKey"`
	V1  int64 `zorm:"version"`
	V2  int64 `zorm:"version"`
	Pad int
}

func (doubleVersion) TableName() string { return "double_version" }

func TestParseModel_VersionField_NonNumericRejected(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for non-numeric version field, got none")
		}
	}()
	_ = ParseModel[badVersionType]()
}

func TestParseModel_DuplicateVersionFields_Rejected(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for duplicate version fields, got none")
		}
	}()
	_ = ParseModel[doubleVersion]()
}
