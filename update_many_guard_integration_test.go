package zorm

import (
	"context"
	"errors"
	"testing"
)

func TestUpdateMany_RequiresWhere(t *testing.T) {
	for _, emptyStruct := range []bool{false, true} {
		db := setupSaveDB(t)
		ctx := context.Background()
		if _, err := db.Exec("UPDATE save_items SET name = 'original' WHERE id = 1"); err != nil {
			t.Fatal(err)
		}
		m := New[saveItem]().SetDB(db)
		if emptyStruct {
			m.Where(&saveItem{})
		}
		if err := m.UpdateMany(ctx, map[string]any{"name": "changed"}); !errors.Is(err, ErrInvalidModel) {
			t.Errorf("unguarded update accepted: %v", err)
		}
		var name string
		if err := db.QueryRow("SELECT name FROM save_items WHERE id = 1").Scan(&name); err != nil {
			t.Fatal(err)
		}
		if name != "original" {
			t.Errorf("unguarded write changed row: %s", name)
		}
		db.Close()
	}
}

func TestForceUpdateAll_ExplicitAndScoped(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	ctx := context.Background()
	m := New[saveItem]().SetDB(db)
	if err := m.ForceUpdateAll(ctx, map[string]any{"name": "all"}); err != nil {
		t.Fatal(err)
	}
	if err := m.Clone().Where("id", 1).ForceUpdateAll(ctx, map[string]any{"name": "one"}); err != nil {
		t.Fatal(err)
	}
	var first, second string
	if err := db.QueryRow("SELECT name FROM save_items WHERE id = 1").Scan(&first); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT name FROM save_items WHERE id = 2").Scan(&second); err != nil {
		t.Fatal(err)
	}
	if first != "one" || second != "all" {
		t.Fatalf("explicit/scoped update: %q, %q", first, second)
	}
	if err := m.Where("bad;column", 1).ForceUpdateAll(ctx, map[string]any{"name": "invalid"}); !errors.Is(err, ErrInvalidColumnName) {
		t.Errorf("forced update ignored build error: %v", err)
	}
}
