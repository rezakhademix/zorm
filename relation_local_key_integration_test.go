package zorm

import (
	"context"
	"reflect"
	"testing"
)

type localKeyParent struct {
	ID       int
	Code     string
	Children []*localKeyChild
	Profile  *localKeyChild
}

func (localKeyParent) TableName() string { return "local_key_parents" }
func (localKeyParent) ChildrenRelation() HasMany[localKeyChild] {
	return HasMany[localKeyChild]{ForeignKey: "parent_code", LocalKey: "code"}
}
func (localKeyParent) ProfileRelation() HasOne[localKeyChild] {
	return HasOne[localKeyChild]{ForeignKey: "parent_code", LocalKey: "code"}
}

type localKeyChild struct {
	ID         int
	ParentCode string
}

func (localKeyChild) TableName() string { return "local_key_children" }

func TestRelations_HasManyAndHasOneHonorLocalKey(t *testing.T) {
	for _, path := range []string{"eager", "lazy", "dynamic"} {
		t.Run(path, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			_, err := db.Exec(`CREATE TABLE local_key_parents (id INTEGER PRIMARY KEY, code TEXT);
				CREATE TABLE local_key_children (id INTEGER PRIMARY KEY, parent_code TEXT);
				INSERT INTO local_key_parents VALUES (1, 'A'), (2, 'B');
				INSERT INTO local_key_children VALUES (1, 'A'), (2, 'B'), (3, '1')`)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			m := New[localKeyParent]().SetDB(db).OrderBy("id", "ASC")
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
				for i, parent := range parents {
					boxed[i] = parent
				}
				err = m.loadRelationsDynamic(ctx, boxed, reflect.TypeFor[localKeyParent](), []string{"Children", "Profile"})
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, parent := range parents {
				defer ClearOriginals(parent)
				if len(parent.Children) != 1 || parent.Children[0].ID != parent.ID {
					t.Errorf("parent %s loaded wrong children: %+v", parent.Code, parent.Children)
				}
				if parent.Profile == nil || parent.Profile.ID != parent.ID {
					t.Errorf("parent %s loaded wrong profile: %+v", parent.Code, parent.Profile)
				}
				for _, child := range parent.Children {
					defer ClearOriginals(child)
				}
				defer ClearOriginals(parent.Profile)
			}
		})
	}
}
