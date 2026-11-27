package zorm

import (
	"context"
	"errors"
	"testing"
)

func TestJoinedWrites_RejectUnsupportedScope(t *testing.T) {
	testRejectedWriteScope(t, func(m *Model[saveItem]) *Model[saveItem] {
		return m.Join("allowed_items", "allowed_items.item_id", "=", "save_items.id")
	})
}

func TestRawWrites_RejectConflictingQueryState(t *testing.T) {
	testRejectedWriteScope(t, func(m *Model[saveItem]) *Model[saveItem] {
		return m.Raw("SELECT * FROM save_items WHERE id = ?", 1)
	})
}

// Unsupported query state must fail before any write or entity mutation.
func testRejectedWriteScope(t *testing.T, scope func(*Model[saveItem]) *Model[saveItem]) {
	t.Helper()
	for _, operation := range []string{"Update", "UpdateColumns", "Save", "UpdateMany", "UpdateManyByKey", "Delete", "DeleteMany", "ForceDeleteAll"} {
		t.Run(operation, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			if _, err := db.Exec(`CREATE TABLE allowed_items (item_id INTEGER); INSERT INTO allowed_items VALUES (1)`); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			base := New[saveItem]().SetDB(db)
			entity, err := base.Find(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer ClearOriginals(entity)
			entity.Name = "changed"
			m := scope(base.Where("value", ">", 0))
			switch operation {
			case "Update":
				err = m.Update(ctx, entity)
			case "UpdateColumns":
				err = m.UpdateColumns(ctx, entity, "name")
			case "Save":
				err = m.Save(ctx, entity)
			case "UpdateMany":
				err = m.UpdateMany(ctx, map[string]any{"name": "changed"})
			case "UpdateManyByKey":
				err = m.UpdateManyByKey(ctx, "id", "name", map[int]string{1: "changed", 2: "changed"})
			case "Delete":
				err = m.Delete(ctx)
			case "DeleteMany":
				err = m.DeleteMany(ctx)
			case "ForceDeleteAll":
				err = m.ForceDeleteAll(ctx)
			}
			if !errors.Is(err, ErrInvalidModel) {
				t.Errorf("expected ErrInvalidModel, got %v", err)
			}
			rows, err := New[saveItem]().SetDB(db).OrderBy("id", "ASC").Get(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 2 || rows[0].Name != "A" || rows[1].Name != "B" {
				t.Fatalf("unsupported write changed stored rows: %+v", rows)
			}
		})
	}
}
