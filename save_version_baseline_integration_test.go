package zorm

import (
	"context"
	"errors"
	"testing"
)

func TestSave_RejectsModifiedLoadedVersion(t *testing.T) {
	for _, dataChanged := range []bool{false, true} {
		db := setupSaveDB(t)
		ctx := context.Background()
		m := New[saveVersionedItem]().SetDB(db)
		entity, err := m.Find(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.Exec("UPDATE save_versioned_items SET name = 'concurrent', version = 2 WHERE id = 1")
		if err != nil {
			t.Fatal(err)
		}
		entity.Version = 2
		if dataChanged {
			entity.Name = "stale"
		}
		if err := m.Save(ctx, entity); !errors.Is(err, ErrOptimisticLock) {
			t.Errorf("dataChanged=%v: mutated version bypassed the baseline: %v", dataChanged, err)
		}
		var name string
		var version int64
		if err := db.QueryRow("SELECT name, version FROM save_versioned_items WHERE id = 1").Scan(&name, &version); err != nil {
			t.Fatal(err)
		}
		if name != "concurrent" || version != 2 {
			t.Errorf("stale entity overwrote concurrent write: name=%q, version=%d", name, version)
		}
		if GetOriginal(entity, "version") != int64(1) {
			t.Error("rejected save changed the original version")
		}
		ClearOriginals(entity)
		db.Close()
	}
}
