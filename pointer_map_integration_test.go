package zorm

import (
	"context"
	"testing"
)

type pointerMapRecord struct {
	ID    int
	Name  string
	Value *int
}

func (pointerMapRecord) TableName() string { return "save_items" }

func TestLookupCreate_AssignsPointerMapValues(t *testing.T) {
	value := 42
	for _, operation := range []string{"FirstOrCreate", "UpdateOrCreate"} {
		for _, pointer := range []*int{&value, nil} {
			db := setupSaveDB(t)
			m := New[pointerMapRecord]().SetDB(db)
			var item *pointerMapRecord
			var err error
			if operation == "FirstOrCreate" {
				item, err = m.FirstOrCreate(context.Background(), map[string]any{"name": "new"}, map[string]any{"value": pointer})
			} else {
				item, err = m.UpdateOrCreate(context.Background(), map[string]any{"name": "A"}, map[string]any{"value": pointer})
			}
			if err != nil {
				db.Close()
				t.Errorf("%s pointer=%v: %v", operation, pointer, err)
				continue
			}
			defer ClearOriginals(item)
			var stored *int
			if err := db.QueryRow("SELECT value FROM save_items WHERE id = ?", item.ID).Scan(&stored); err != nil {
				t.Fatal(err)
			}
			if (stored == nil) != (pointer == nil) || (stored != nil && *stored != *pointer) {
				t.Errorf("%s stored wrong pointer value: %v", operation, stored)
			}
			db.Close()
		}
	}
}
