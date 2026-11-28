package zorm

import (
	"context"
	"testing"
)

type callbackPathParent struct {
	ID       int
	Children []*callbackPathChild
}

func (callbackPathParent) TableName() string { return "cb_parents" }
func (callbackPathParent) ChildrenRelation() HasMany[callbackPathChild] {
	return HasMany[callbackPathChild]{ForeignKey: "parent_id"}
}

type callbackPathChild struct {
	ID       int
	ParentID int
	Visible  bool
	Comments []*callbackPathComment
}

func (callbackPathChild) TableName() string { return "cb_children" }
func (callbackPathChild) CommentsRelation() HasMany[callbackPathComment] {
	return HasMany[callbackPathComment]{ForeignKey: "child_id"}
}

type callbackPathComment struct {
	ID      int
	ChildID int
	Visible bool
}

func (callbackPathComment) TableName() string { return "cb_comments" }

func TestWithCallback_PathsApplyConstraints(t *testing.T) {
	for _, path := range []string{"Children:id,parent_id,visible", "Children.Comments", "Children.Comments:id,child_id,visible"} {
		t.Run(path, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			_, err := db.Exec(`CREATE TABLE cb_parents (id INTEGER PRIMARY KEY);
			CREATE TABLE cb_children (id INTEGER PRIMARY KEY, parent_id INTEGER, visible BOOLEAN);
			CREATE TABLE cb_comments (id INTEGER PRIMARY KEY, child_id INTEGER, visible BOOLEAN);
			INSERT INTO cb_parents VALUES (1);
			INSERT INTO cb_children VALUES (1, 1, 1), (2, 1, 0);
			INSERT INTO cb_comments VALUES (1, 1, 1), (2, 1, 0), (3, 2, 1), (4, 2, 0)`)
			if err != nil {
				t.Fatal(err)
			}
			m := New[callbackPathParent]().SetDB(db)
			if path == "Children:id,parent_id,visible" {
				m.WithCallback(path, func(q *Model[callbackPathChild]) { q.Where("visible", true) })
			} else {
				m.WithCallback(path, func(q *Model[callbackPathComment]) { q.Where("visible", true).OrderBy("id", "DESC").Limit(1) })
			}
			rows, err := m.Get(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 {
				t.Fatalf("parents: %d", len(rows))
			}
			defer ClearOriginals(rows[0])
			if path == "Children:id,parent_id,visible" && len(rows[0].Children) != 1 {
				t.Errorf("loaded unfiltered children: %d", len(rows[0].Children))
			}
			for _, child := range rows[0].Children {
				defer ClearOriginals(child)
				if path != "Children:id,parent_id,visible" && (len(child.Comments) != 1 || !child.Comments[0].Visible) {
					t.Errorf("loaded unfiltered comments: %+v", child.Comments)
				}
				for _, comment := range child.Comments {
					defer ClearOriginals(comment)
				}
			}
		})
	}
}
