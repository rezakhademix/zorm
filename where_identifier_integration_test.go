package zorm

import (
	"context"
	"errors"
	"testing"
)

func TestWhereIdentifier_RejectsSQLInColumnForms(t *testing.T) {
	for _, column := range []string{"id = 1 OR 1=1 OR id", "name IS NULL OR TRUE OR name", "id -- ignored", "id, name", "id) OR TRUE OR (id"} {
		t.Run(column, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			ctx := context.Background()
			for _, build := range []func() *Model[saveItem]{
				func() *Model[saveItem] { return New[saveItem]().SetDB(db).Where(column, 1) },
				func() *Model[saveItem] { return New[saveItem]().SetDB(db).OrWhere(column, "=", 1) },
				func() *Model[saveItem] { return New[saveItem]().SetDB(db).Where(map[string]any{column: 1}) },
			} {
				if err := build().Delete(ctx); !errors.Is(err, ErrInvalidColumnName) {
					t.Errorf("unsafe model column accepted: %v", err)
				}
			}
			q := Query[int]().SetDB(db).Table("save_items").Select("id").Where(column, 1)
			if _, err := q.Get(ctx); !errors.Is(err, ErrInvalidColumnName) {
				t.Errorf("unsafe scalar column accepted: %v", err)
			}
			count, err := New[saveItem]().SetDB(db).Count(ctx)
			if err != nil || count != 2 {
				t.Fatalf("rejected column changed rows: count=%d, err=%v", count, err)
			}
		})
	}
}

func TestWhereIdentifier_PreservesIdentifiersOperatorsAndRawExpressions(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	ctx := context.Background()
	for _, column := range []string{"save_items.id", "id >"} {
		rows, err := New[saveItem]().SetDB(db).Where(column, 1).Get(ctx)
		if err != nil || len(rows) != 1 {
			t.Errorf("valid condition %q failed: rows=%v, err=%v", column, rows, err)
		}
	}
	rows, err := New[saveItem]().SetDB(db).Where("LOWER(name) = ?", "a").Get(ctx)
	if err != nil || len(rows) != 1 || rows[0].ID != 1 {
		t.Fatalf("trusted raw expression failed: rows=%v, err=%v", rows, err)
	}
}
