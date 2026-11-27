package zorm

import (
	"context"
	"testing"
)

type evictionSaveItem struct {
	ID     int
	Name   string
	Value  int
	Before func() error `zorm:"-"`
}

func (evictionSaveItem) TableName() string { return "save_items" }

func (e *evictionSaveItem) BeforeUpdate(context.Context) error {
	if e.Before != nil {
		return e.Before()
	}
	return nil
}

// Loading enough entities inside BeforeUpdate can evict the entity being saved.
// Save must keep the validated baseline for both its diff and its final sync.
func TestSave_EvictionCannotWidenPartialWrite(t *testing.T) {
	for _, excluded := range []bool{false, true} {
		old := globalTracker.Load()
		ConfigureDirtyTracking(256)
		func() {
			defer globalTracker.Store(old)
			db := setupSaveDB(t)
			defer db.Close()
			_, err := db.Exec(`
				WITH RECURSIVE ids(id) AS (
					SELECT 3 UNION ALL SELECT id + 1 FROM ids WHERE id < 600
				)
				INSERT INTO save_items SELECT id, 'filler', 10 FROM ids
			`)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			m := New[evictionSaveItem]().SetDB(db)
			entity, err := m.Clone().Select("id", "name").Find(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer ClearOriginals(entity)
			entity.Name = "changed"
			if excluded {
				entity.Value = 999
				m.Exclude("value")
			}
			entity.Before = func() error {
				if _, err := New[evictionSaveItem]().SetDB(db).Get(ctx); err != nil {
					return err
				}
				if IsTracked(entity) {
					t.Fatal("test did not evict the entity's baseline")
				}
				return nil
			}
			if err := m.Save(ctx, entity); err != nil {
				t.Fatal(err)
			}
			var value int
			if err := db.QueryRow("SELECT value FROM save_items WHERE id = 1").Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != 10 {
				t.Errorf("excluded=%v: eviction overwrote an unwritten column with %d", excluded, value)
			}
			if !IsTracked(entity) || m.IsDirtyField(entity, "name") {
				t.Error("Save did not retain its validated baseline after eviction")
			}
			if excluded && !m.IsDirtyField(entity, "value") {
				t.Error("eviction lost the excluded pending change")
			}
		}()
	}
}
