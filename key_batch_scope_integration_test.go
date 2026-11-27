package zorm

import (
	"context"
	"strconv"
	"testing"
)

// The lookup-key restriction applies to the entire caller predicate. Without
// grouping, an OR branch can reach rows absent from CASE and set them to NULL.
func TestUpdateManyByKey_OrScopeCannotEscapeLookupKeys(t *testing.T) {
	for _, size := range []int{1, 501} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			ctx := context.Background()
			updates := make(map[int]string, size)
			for id := 2; id < size+2; id++ {
				updates[id] = "changed"
			}
			m := New[saveItem]().SetDB(db).Where("value", 10).OrWhere("value", 20)
			if err := m.UpdateManyByKey(ctx, "id", "name", updates); err != nil {
				t.Fatal(err)
			}
			for id, want := range map[int]string{1: "A", 2: "changed"} {
				var name *string
				if err := db.QueryRow("SELECT name FROM save_items WHERE id = ?", id).Scan(&name); err != nil {
					t.Fatal(err)
				}
				if name == nil || *name != want {
					t.Errorf("id %d: name=%v, want %q", id, name, want)
				}
			}
		})
	}
}
