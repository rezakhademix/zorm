package zorm

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestUpdateColumns_UnknownColumnFailsBeforeTouch(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	ctx := context.Background()
	item := &saveItem{ID: 1, Name: "changed"}
	if err := New[saveItem]().SetDB(db).UpdateColumns(ctx, item, "name", "emial"); !errors.Is(err, ErrColumnNotFound) {
		t.Errorf("unknown column accepted: %v", err)
	}
	var name string
	if err := db.QueryRow("SELECT name FROM save_items WHERE id = 1").Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "A" {
		t.Errorf("partially applied invalid update: %s", name)
	}
	touched := &saveItemWithTouch{ID: 1, Name: "changed"}
	if err := New[saveItemWithTouch]().SetDB(db).UpdateColumns(ctx, touched, "emial"); !errors.Is(err, ErrColumnNotFound) {
		t.Errorf("unknown touch column accepted: %v", err)
	}
	if touched.UpdatedAt != (time.Time{}) {
		t.Error("invalid update mutated updated_at")
	}
}
