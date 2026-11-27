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

// Evict the entity's baseline inside BeforeUpdate through the tracker's LRU.
// Save must keep the validated baseline for both its diff and its final sync.
func TestSave_EvictionCannotWidenPartialWrite(t *testing.T) {
	for _, excluded := range []bool{false, true} {
		old := globalTracker.Load()
		ConfigureDirtyTracking(shardCount)
		func() {
			defer globalTracker.Store(old)
			db := setupSaveDB(t)
			defer db.Close()
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
				// One entry fits per shard. A key offset by shardCount maps to
				// the same shard, guaranteeing eviction without allocator assumptions.
				key := getEntityKey(entity) + uintptr(shardCount)
				globalTracker.Load().Store(key, map[string]any{"name": "filler"}, nil)
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
