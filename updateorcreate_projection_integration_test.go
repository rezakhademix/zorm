package zorm

import (
	"context"
	"testing"
)

// A projected lookup leaves unselected struct fields zero. Updating an
// existing row must only persist the columns supplied in values.
func TestUpdateOrCreate_PreservesUnselectedColumns(t *testing.T) {
	for _, mode := range []string{"direct", "cached", "transaction"} {
		t.Run(mode, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			ctx := context.Background()
			m := New[saveItem]().SetDB(db).Select("id", "name")
			if mode == "cached" {
				cache := NewStmtCache(10)
				defer cache.Close()
				m.WithStmtCache(cache)
			}
			write := func(q *Model[saveItem]) error {
				entity, err := q.UpdateOrCreate(ctx, map[string]any{"id": 1}, map[string]any{"name": "changed"})
				if entity != nil {
					defer ClearOriginals(entity)
				}
				return err
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
			var name string
			var value int
			if err := db.QueryRow(`SELECT name, value FROM save_items WHERE id = 1`).Scan(&name, &value); err != nil {
				t.Fatal(err)
			}
			if name != "changed" || value != 10 {
				t.Fatalf("stored name=%q value=%d, want changed/10", name, value)
			}
		})
	}
}
