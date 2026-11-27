package zorm

import (
	"context"
	"reflect"
	"testing"
)

type nullableOwnerRecord struct {
	ID   int
	Code int
}

func (nullableOwnerRecord) TableName() string { return "nullable_owner_records" }

type nullableOwnerChild struct {
	ID        int
	OwnerCode *int
	Owner     *nullableOwnerRecord
}

func (nullableOwnerChild) TableName() string { return "nullable_owner_children" }
func (nullableOwnerChild) OwnerRelation() BelongsTo[nullableOwnerRecord] {
	return BelongsTo[nullableOwnerRecord]{ForeignKey: "owner_code", OwnerKey: "code"}
}

func TestBelongsTo_NonNilZeroKeyRemainsPresent(t *testing.T) {
	for _, path := range []string{"eager", "lazy", "dynamic"} {
		t.Run(path, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			_, err := db.Exec(`CREATE TABLE nullable_owner_records (id INTEGER PRIMARY KEY, code INTEGER);
				CREATE TABLE nullable_owner_children (id INTEGER PRIMARY KEY, owner_code INTEGER);
				INSERT INTO nullable_owner_records VALUES (1, 0), (2, 10);
				INSERT INTO nullable_owner_children VALUES (1, 0), (2, 10), (3, NULL)`)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			m := New[nullableOwnerChild]().SetDB(db).OrderBy("id", "ASC")
			if path == "eager" {
				m.With("Owner")
			}
			children, err := m.Get(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if path == "lazy" {
				err = m.LoadSlice(ctx, children, "Owner")
			}
			if path == "dynamic" {
				boxed := make([]any, len(children))
				for i, child := range children {
					boxed[i] = child
				}
				err = m.loadRelationsDynamic(ctx, boxed, reflect.TypeFor[nullableOwnerChild](), []string{"Owner"})
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, child := range children {
				defer ClearOriginals(child)
				defer ClearOriginals(child.Owner)
				if child.ID == 3 {
					if child.Owner != nil {
						t.Error("NULL foreign key acquired an owner")
					}
				} else if child.Owner == nil || child.Owner.ID != child.ID {
					t.Errorf("non-NULL key lost its owner: child=%+v", child)
				}
			}
		})
	}
}
