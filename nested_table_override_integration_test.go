package zorm

import (
	"context"
	"testing"
)

type nestedOverrideRoot struct {
	ID      int
	Parents []*nestedOverrideParent
}

func (nestedOverrideRoot) TableName() string { return "nested_override_roots" }
func (nestedOverrideRoot) ParentsRelation() HasMany[nestedOverrideParent] {
	return HasMany[nestedOverrideParent]{ForeignKey: "root_id"}
}

type nestedOverrideParent struct {
	ID       int
	RootID   int
	Children []*nestedOverrideChild
	Child    *nestedOverrideChild
}

func (nestedOverrideParent) TableName() string { return "nested_override_parents" }
func (nestedOverrideParent) ChildrenRelation() HasMany[nestedOverrideChild] {
	return HasMany[nestedOverrideChild]{ForeignKey: "parent_id", Table: "nested_override_children"}
}
func (nestedOverrideParent) ChildRelation() HasOne[nestedOverrideChild] {
	return HasOne[nestedOverrideChild]{ForeignKey: "parent_id", Table: "nested_override_children"}
}

type nestedOverrideChild struct {
	ID       int
	ParentID int
}

func (nestedOverrideChild) TableName() string { return "nested_default_children" }

// Nested loading must use the same configured table as a direct relation load.
func TestNestedRelations_HonorTableOverride(t *testing.T) {
	for _, relation := range []string{"Children", "Child"} {
		for _, loader := range []string{"With", "Load", "LoadSlice"} {
			t.Run(relation+"/"+loader, func(t *testing.T) {
				db := setupSaveDB(t)
				defer db.Close()
				_, err := db.Exec(`CREATE TABLE nested_override_roots (id INTEGER PRIMARY KEY);
					CREATE TABLE nested_override_parents (id INTEGER PRIMARY KEY, root_id INTEGER);
					CREATE TABLE nested_default_children (id INTEGER PRIMARY KEY, parent_id INTEGER);
					CREATE TABLE nested_override_children (id INTEGER PRIMARY KEY, parent_id INTEGER);
					INSERT INTO nested_override_roots VALUES (1);
					INSERT INTO nested_override_parents VALUES (1, 1);
					INSERT INTO nested_default_children VALUES (1, 1);
					INSERT INTO nested_override_children VALUES (99, 1)`)
				if err != nil {
					t.Fatal(err)
				}
				ctx := context.Background()
				m := New[nestedOverrideRoot]().SetDB(db)
				root := &nestedOverrideRoot{ID: 1}
				path := "Parents." + relation
				switch loader {
				case "With":
					root, err = m.With(path).Find(ctx, 1)
				case "Load":
					err = m.Load(ctx, root, path)
				case "LoadSlice":
					err = m.LoadSlice(ctx, []*nestedOverrideRoot{root}, path)
				}
				if err != nil {
					t.Fatal(err)
				}
				if len(root.Parents) != 1 {
					t.Fatal("expected one parent")
				}
				parent := root.Parents[0]
				if relation == "Children" {
					if len(parent.Children) != 1 || parent.Children[0].ID != 99 {
						t.Fatal("nested has-many read the default table")
					}
				} else if parent.Child == nil || parent.Child.ID != 99 {
					t.Fatal("nested has-one read the default table")
				}
			})
		}
	}
}
