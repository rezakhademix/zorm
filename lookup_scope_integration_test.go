package zorm

import (
	"context"
	"testing"
)

// Mandatory lookup predicates must constrain every branch of the caller's scope.
func TestLookups_GroupOrScope(t *testing.T) {
	for _, operation := range []string{"Find", "FirstOrCreate", "UpdateOrCreate"} {
		t.Run(operation, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			ctx := context.Background()
			m := New[saveItem]().SetDB(db).Where("name", "A").OrWhere("name", "B")
			var entity *saveItem
			var err error
			switch operation {
			case "Find":
				entity, err = m.Find(ctx, 2)
			case "FirstOrCreate":
				entity, err = m.FirstOrCreate(ctx, map[string]any{"id": 2}, nil)
			case "UpdateOrCreate":
				entity, err = m.UpdateOrCreate(ctx, map[string]any{"id": 2}, map[string]any{"name": "changed"})
			}
			if err != nil {
				t.Fatal(err)
			}
			defer ClearOriginals(entity)
			if entity.ID != 2 {
				t.Fatalf("%s returned ID %d, want 2", operation, entity.ID)
			}
			if len(m.wheres) != 2 || len(m.args) != 2 {
				t.Fatal("lookup mutated the original scope")
			}
		})
	}
}
