package zorm

import (
	"context"
	"testing"
)

func TestRawWhere_OrCannotEscapeFollowingFilter(t *testing.T) {
	for _, operation := range []string{"get", "scalar", "delete", "update"} {
		t.Run(operation, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			ctx := context.Background()
			m := New[saveItem]().SetDB(db).Where("id = ? OR value = ?", 1, 20).Where("name", "B")
			switch operation {
			case "get":
				rows, err := m.Get(ctx)
				if err != nil {
					t.Fatal(err)
				}
				for _, row := range rows {
					defer ClearOriginals(row)
				}
				if len(rows) != 1 || rows[0].ID != 2 {
					t.Fatalf("OR escaped filter: %+v", rows)
				}
			case "scalar":
				ids, err := Query[int]().SetDB(db).Table("save_items").Select("id").Where("id = ? OR value = ?", 1, 20).Where("name", "B").Get(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if len(ids) != 1 || ids[0] != 2 {
					t.Fatalf("OR escaped scalar filter: %v", ids)
				}
			case "delete", "update":
				var err error
				if operation == "delete" {
					err = m.Delete(ctx)
				} else {
					err = m.UpdateMany(ctx, map[string]any{"name": "changed"})
				}
				if err != nil {
					t.Fatal(err)
				}
				var name string
				if err := db.QueryRow("SELECT name FROM save_items WHERE id = 1").Scan(&name); err != nil {
					t.Fatal(err)
				}
				if name != "A" {
					t.Fatalf("write escaped filter: %s", name)
				}
			}
		})
	}
}
