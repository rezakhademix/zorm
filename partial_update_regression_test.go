package zorm

import (
	"context"
	"testing"
)

func TestPartialUpdate_ExcludedChangesRemainDirty(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	ctx := context.Background()
	m := New[saveItem]().SetDB(db)
	entity, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(entity)
	entity.Name = "changed"
	entity.Value = 999
	if err := m.UpdateColumns(ctx, entity, "name"); err != nil {
		t.Fatal(err)
	}
	if m.IsDirtyField(entity, "name") {
		t.Error("persisted name must be clean")
	}
	if !m.IsDirtyField(entity, "value") {
		t.Error("excluded value must remain dirty")
	}
	if err := m.Save(ctx, entity); err != nil {
		t.Fatal(err)
	}
	var name string
	var value int
	if err := db.QueryRow(`SELECT name, value FROM save_items WHERE id = 1`).Scan(&name, &value); err != nil {
		t.Fatal(err)
	}
	if name != "changed" || value != 999 {
		t.Errorf("Save lost pending change: got (%q, %d), want (changed, 999)", name, value)
	}
}

func TestPartialUpdate_ExcludePreservesPendingChanges(t *testing.T) {
	for _, operation := range []string{"Update", "Save"} {
		t.Run(operation, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			ctx := context.Background()
			m := New[saveItem]().SetDB(db)
			entity, err := m.Find(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer ClearOriginals(entity)
			entity.Name, entity.Value = "changed", 999
			partial := New[saveItem]().SetDB(db).Exclude("value")
			if operation == "Update" {
				err = partial.Update(ctx, entity)
			} else {
				err = partial.Save(ctx, entity)
			}
			if err != nil {
				t.Fatal(err)
			}
			if !m.IsDirtyField(entity, "value") {
				t.Error("excluded value must remain dirty")
			}
			if err := m.Save(ctx, entity); err != nil {
				t.Fatal(err)
			}
			var stored int
			if err := db.QueryRow(`SELECT value FROM save_items WHERE id = 1`).Scan(&stored); err != nil {
				t.Fatal(err)
			}
			if stored != 999 {
				t.Errorf("Save lost excluded change: stored %d, want 999", stored)
			}
		})
	}
}
