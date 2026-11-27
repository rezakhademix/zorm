package zorm

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

type excludedTouchItem struct {
	ID        int
	Name      string
	UpdatedAt time.Time
	Version   int64 `zorm:"version"`
	Calls     int   `zorm:"-"`
}

func (excludedTouchItem) TableName() string { return "save_items_touch" }
func (item *excludedTouchItem) AfterUpdate(context.Context) error {
	item.Calls++
	return nil
}

func setupExcludedTouchDB(t *testing.T) *sql.DB {
	t.Helper()
	db := setupSaveDB(t)
	_, err := db.Exec(`ALTER TABLE save_items_touch ADD COLUMN version INTEGER NOT NULL DEFAULT 1;
		CREATE TABLE excluded_touch_writes (n INTEGER);
		INSERT INTO excluded_touch_writes VALUES (0);
		CREATE TRIGGER count_excluded_touch_writes AFTER UPDATE ON save_items_touch
		BEGIN UPDATE excluded_touch_writes SET n = n + 1; END`)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// Excluded-only changes remain pending and must not touch timestamps or versions.
func TestSave_ExcludedOnlyRemainsNoOp(t *testing.T) {
	for _, mode := range []string{"direct", "cached", "transaction"} {
		t.Run(mode, func(t *testing.T) {
			db := setupExcludedTouchDB(t)
			defer db.Close()
			ctx := context.Background()
			m := New[excludedTouchItem]().SetDB(db)
			if mode == "cached" {
				cache := NewStmtCache(10)
				defer cache.Close()
				m.WithStmtCache(cache)
			}
			entity, err := m.Find(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer ClearOriginals(entity)
			before := entity.UpdatedAt
			entity.Name = "pending"
			m.Exclude("name")
			if mode == "transaction" {
				err = m.Transaction(ctx, func(tx *Tx) error { return m.WithTx(tx).Save(ctx, entity) })
			} else {
				err = m.Save(ctx, entity)
			}
			if err != nil {
				t.Fatal(err)
			}
			var name string
			var at time.Time
			var version, writes int
			if err := db.QueryRow(`SELECT name, updated_at, version FROM save_items_touch WHERE id = 1`).Scan(&name, &at, &version); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT n FROM excluded_touch_writes`).Scan(&writes); err != nil {
				t.Fatal(err)
			}
			if name != "A" || !at.Equal(before) || version != 1 || writes != 0 {
				t.Errorf("excluded-only save wrote data: name=%q at=%v version=%d writes=%d", name, at, version, writes)
			}
			if !entity.UpdatedAt.Equal(before) || entity.Version != 1 || entity.Calls != 1 || !m.IsDirtyField(entity, "name") {
				t.Errorf("incorrect no-op entity state: %+v", entity)
			}
		})
	}
}

func TestSave_ExcludedTimestampPreservesExplicitValue(t *testing.T) {
	db := setupExcludedTouchDB(t)
	defer db.Close()
	ctx := context.Background()
	m := New[excludedTouchItem]().SetDB(db)
	entity, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(entity)
	before := entity.UpdatedAt
	explicit := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	entity.Name, entity.UpdatedAt = "changed", explicit
	if err := m.Exclude("updated_at").Save(ctx, entity); err != nil {
		t.Fatal(err)
	}
	var stored time.Time
	if err := db.QueryRow(`SELECT updated_at FROM save_items_touch WHERE id = 1`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if !stored.Equal(before) || !entity.UpdatedAt.Equal(explicit) || entity.Version != 2 {
		t.Fatalf("explicit excluded timestamp was overwritten: stored=%v entity=%+v", stored, entity)
	}
}

func TestSave_ExcludedAutomaticTimestampIsNotWritten(t *testing.T) {
	db := setupExcludedTouchDB(t)
	defer db.Close()
	ctx := context.Background()
	m := New[excludedTouchItem]().SetDB(db)
	entity, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(entity)
	before := entity.UpdatedAt
	entity.Name = "changed"
	if err := m.Exclude("updated_at").Save(ctx, entity); err != nil {
		t.Fatal(err)
	}
	var stored time.Time
	if err := db.QueryRow(`SELECT updated_at FROM save_items_touch WHERE id = 1`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if !stored.Equal(before) || entity.Version != 2 || !m.IsDirtyField(entity, "updated_at") {
		t.Fatalf("automatic timestamp did not respect exclusion: stored=%v entity=%+v", stored, entity)
	}
}
