package zorm

import (
	"context"
	"database/sql"
	"testing"
)

type snapshotItem struct {
	ID    int `zorm:"primaryKey"`
	Value *int
}

func (snapshotItem) TableName() string { return "snapshot_items" }

func TestDirtySnapshot_InPlacePointerMutation(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE snapshot_items (id INTEGER PRIMARY KEY, value INTEGER);
		INSERT INTO snapshot_items VALUES (1, 10)`); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	m := New[snapshotItem]().SetDB(db)
	entity, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(entity)
	*entity.Value = 20
	if original := GetOriginal(entity, "value").(*int); *original != 10 {
		t.Errorf("original changed with entity: got %d, want 10", *original)
	}
	if !m.IsDirtyField(entity, "value") {
		t.Error("in-place pointer mutation must be dirty")
	}
	if err := m.Save(ctx, entity); err != nil {
		t.Fatal(err)
	}
	var stored int
	if err := db.QueryRow(`SELECT value FROM snapshot_items WHERE id = 1`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != 20 {
		t.Errorf("Save lost pointer mutation: stored %d, want 20", stored)
	}
}
