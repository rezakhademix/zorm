package zorm

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

type baselineHookChild struct {
	ID     int
	UserID int
	Title  string
}

func (baselineHookChild) TableName() string { return "rel_posts" }
func (p *baselineHookChild) AfterFind(context.Context) error {
	p.Title = strings.ToUpper(p.Title)
	return nil
}

type baselineHookParent struct {
	ID       int
	Children []*baselineHookChild
}

func (baselineHookParent) TableName() string { return "rel_users" }
func (baselineHookParent) ChildrenRelation() HasMany[baselineHookChild] {
	return HasMany[baselineHookChild]{ForeignKey: "user_id"}
}

func TestRelationBaseline_PrecedesAfterFind(t *testing.T) {
	for _, path := range []string{"direct", "eager", "lazy", "nested"} {
		t.Run(path, func(t *testing.T) {
			db := setupRelDB(t)
			defer db.Close()
			ctx := context.Background()
			m := New[baselineHookChild]().SetDB(db)
			parents := New[baselineHookParent]().SetDB(db)
			parent := &baselineHookParent{ID: 1}
			var child *baselineHookChild
			var err error
			switch path {
			case "direct":
				child, err = m.Find(ctx, 1)
			case "eager":
				parent, err = parents.With("Children").Find(ctx, 1)
			case "lazy":
				err = parents.Load(ctx, parent, "Children")
			case "nested":
				err = parents.loadRelationsDynamic(ctx, []any{parent}, reflect.TypeFor[baselineHookParent](), []string{"Children"})
			}
			if err != nil {
				t.Fatal(err)
			}
			if path != "direct" {
				child = parent.Children[0]
			}
			defer ClearOriginals(child)
			if GetOriginal(child, "title") != "Post 1" || !m.IsDirtyField(child, "title") {
				t.Errorf("hook mutation became clean: baseline=%v", GetOriginal(child, "title"))
			}
			if err := m.Save(ctx, child); err != nil {
				t.Fatal(err)
			}
			var title string
			if err := db.QueryRow("SELECT title FROM rel_posts WHERE id = 1").Scan(&title); err != nil {
				t.Fatal(err)
			}
			if title != "POST 1" {
				t.Errorf("hook change was not saved: %q", title)
			}
		})
	}
}
