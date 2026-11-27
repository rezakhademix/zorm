package zorm

import (
	"context"
	"errors"
	"testing"
)

func TestUpdateOrCreate_RejectsPrimaryKeyChange(t *testing.T) {
	for _, mode := range []string{"direct", "cached", "transaction"} {
		t.Run(mode, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			ctx := context.Background()
			m := New[saveItem]().SetDB(db)
			if mode == "cached" {
				cache := NewStmtCache(10)
				defer cache.Close()
				m.WithStmtCache(cache)
			}
			write := func(q *Model[saveItem]) error {
				_, err := q.UpdateOrCreate(ctx, map[string]any{"name": "A"}, map[string]any{"id": 2, "name": "changed"})
				return err
			}
			var err error
			if mode == "transaction" {
				err = m.Transaction(ctx, func(tx *Tx) error { return write(m.WithTx(tx)) })
			} else {
				err = write(m)
			}
			if !errors.Is(err, ErrInvalidModel) {
				t.Errorf("expected ErrInvalidModel, got %v", err)
			}
			for id, want := range map[int]string{1: "A", 2: "B"} {
				var name string
				if err := db.QueryRow(`SELECT name FROM save_items WHERE id = ?`, id).Scan(&name); err != nil {
					t.Fatal(err)
				}
				if name != want {
					t.Errorf("row %d changed to %q, want %q", id, name, want)
				}
			}
		})
	}
}

func TestUpdateOrCreate_AllowsUnchangedPrimaryKey(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	entity, err := New[saveItem]().SetDB(db).UpdateOrCreate(context.Background(), map[string]any{"name": "A"}, map[string]any{"id": "1", "name": "changed"})
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(entity)
	if entity.ID != 1 || entity.Name != "changed" {
		t.Fatalf("unexpected updated entity: %+v", entity)
	}
}
