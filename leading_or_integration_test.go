package zorm

import (
	"context"
	"testing"
)

// A leading OR is the first predicate, not an alternative to an always-true
// sentinel. Reads and writes must apply it just like a leading Where.
func TestLeadingOr_AppliesFirstPredicate(t *testing.T) {
	for _, operation := range []string{"Get", "Count", "Scalar", "Delete", "UpdateMany"} {
		t.Run(operation, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			ctx := context.Background()
			m := New[saveItem]().SetDB(db).OrWhere("id", 1)

			switch operation {
			case "Get":
				rows, err := m.Get(ctx)
				if err != nil || len(rows) != 1 || rows[0].ID != 1 {
					t.Fatalf("Get returned %v, %v; want only id 1", rows, err)
				}
			case "Count":
				count, err := m.Count(ctx)
				if err != nil || count != 1 {
					t.Fatalf("Count = %d, %v; want 1", count, err)
				}
			case "Scalar":
				ids, err := Query[int]().SetDB(db).Table("save_items").Select("id").OrWhere("id", 1).Get(ctx)
				if err != nil || len(ids) != 1 || ids[0] != 1 {
					t.Fatalf("scalar Get = %v, %v; want [1]", ids, err)
				}
			case "Delete":
				if err := m.Delete(ctx); err != nil {
					t.Fatal(err)
				}
				count, err := New[saveItem]().SetDB(db).Count(ctx)
				if err != nil || count != 1 {
					t.Fatalf("remaining rows = %d, %v; want 1", count, err)
				}
			case "UpdateMany":
				if err := m.UpdateMany(ctx, map[string]any{"name": "changed"}); err != nil {
					t.Fatal(err)
				}
				row, err := New[saveItem]().SetDB(db).Find(ctx, 2)
				if err != nil || row.Name != "B" {
					t.Fatalf("unrelated row changed: %v, %v", row, err)
				}
			}
		})
	}
}
