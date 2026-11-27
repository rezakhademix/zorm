package zorm

import (
	"context"
	"testing"
)

func TestColumnMapping_CommasDoNotCollide(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	m := New[saveItem]().SetDB(db)
	if _, err := m.Raw(`SELECT id, name FROM save_items`).Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if p := recover(); p != nil {
			t.Fatalf("column signature collision panicked: %v", p)
		}
	}()
	rows, err := m.Raw(`SELECT 1 AS "id,name"`).Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != 0 || rows[0].Name != "" {
		t.Fatalf("unexpected mapping: %+v", rows)
	}
}
