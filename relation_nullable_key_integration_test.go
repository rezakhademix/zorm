package zorm

import (
	"context"
	"reflect"
	"testing"
)

type nullableKeyParent struct {
	ID       int
	Children []*nullableKeyChild
	Profile  *nullableKeyChild
}

func (nullableKeyParent) TableName() string { return "nullable_key_parents" }
func (nullableKeyParent) ChildrenRelation() HasMany[nullableKeyChild] {
	return HasMany[nullableKeyChild]{ForeignKey: "parent_id"}
}
func (nullableKeyParent) ProfileRelation() HasOne[nullableKeyChild] {
	return HasOne[nullableKeyChild]{ForeignKey: "parent_id"}
}

type nullableKeyChild struct {
	ID       int
	ParentID *int
}

func (nullableKeyChild) TableName() string { return "nullable_key_children" }

func TestRelations_NullableForeignKeysMatchValues(t *testing.T) {
	for _, path := range []string{"eager", "lazy", "dynamic"} {
		t.Run(path, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			_, err := db.Exec(`CREATE TABLE nullable_key_parents (id INTEGER PRIMARY KEY);
				CREATE TABLE nullable_key_children (id INTEGER PRIMARY KEY, parent_id INTEGER);
				INSERT INTO nullable_key_parents VALUES (1), (2);
				INSERT INTO nullable_key_children VALUES (1, 1), (2, 2), (3, NULL)`)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			m := New[nullableKeyParent]().SetDB(db)
			if path == "eager" {
				m.With("Children", "Profile")
			}
			parents, err := m.Get(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if path == "lazy" {
				err = m.LoadSlice(ctx, parents, "Children", "Profile")
			}
			if path == "dynamic" {
				boxed := make([]any, len(parents))
				for i, p := range parents {
					boxed[i] = p
				}
				err = m.loadRelationsDynamic(ctx, boxed, reflect.TypeFor[nullableKeyParent](), []string{"Children", "Profile"})
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, p := range parents {
				defer ClearOriginals(p)
				if len(p.Children) != 1 || p.Children[0].ID != p.ID {
					t.Errorf("parent %d lost nullable children: %+v", p.ID, p.Children)
				}
				if p.Profile == nil || p.Profile.ID != p.ID {
					t.Errorf("parent %d lost nullable profile: %+v", p.ID, p.Profile)
				}
				for _, child := range p.Children {
					defer ClearOriginals(child)
				}
				defer ClearOriginals(p.Profile)
			}
		})
	}
}
