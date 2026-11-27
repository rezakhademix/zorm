package zorm

import (
	"context"
	"strconv"
	"testing"
)

// CTE binds precede CASE and WHERE binds in every batch. A nested CTE and a
// separate WHERE value ensure that dropping or misordering arguments fails.
func TestUpdateManyByKey_KeepsCTEScope(t *testing.T) {
	for _, size := range []int{1, 501} {
		for _, mode := range []string{"direct", "transaction"} {
			t.Run(strconv.Itoa(size)+"/"+mode, func(t *testing.T) {
				db := setupSaveDB(t)
				defer db.Close()
				ctx := context.Background()
				updates := make(map[int]string, size)
				for id := 1; id <= size; id++ {
					updates[id] = "changed"
				}
				inner := New[saveItem]().SetDB(db).Select("id").Where("id", "<=", 1)
				chosen := New[saveItem]().WithCTE("inner_scope", inner).Table("inner_scope").Select("id")
				m := New[saveItem]().SetDB(db).WithCTE("chosen", chosen).
					Where("id IN (SELECT id FROM chosen)").Where("value", 10)
				write := func(q *Model[saveItem]) error {
					return q.UpdateManyByKey(ctx, "id", "name", updates)
				}
				var err error
				if mode == "transaction" {
					err = m.Transaction(ctx, func(tx *Tx) error { return write(m.WithTx(tx)) })
				} else {
					err = write(m)
				}
				if err != nil {
					t.Fatal(err)
				}
				for id, want := range map[int]string{1: "changed", 2: "B"} {
					var name string
					if err := db.QueryRow(`SELECT name FROM save_items WHERE id = ?`, id).Scan(&name); err != nil {
						t.Fatal(err)
					}
					if name != want {
						t.Errorf("id %d: name=%q, want %q", id, name, want)
					}
				}
			})
		}
	}
}
