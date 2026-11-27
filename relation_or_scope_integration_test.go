package zorm

import (
	"context"
	"testing"
)

// OR predicates inside a callback must remain grouped behind the mandatory
// parent-key and morph-type restrictions.
func TestWhereHas_OrCallbackKeepsParentCorrelation(t *testing.T) {
	for _, leadingOr := range []bool{false, true} {
		t.Run(map[bool]string{false: "where then or", true: "leading or"}[leadingOr], func(t *testing.T) {
			db := setupRelDB(t)
			defer db.Close()
			users, err := New[RelUser]().SetDB(db).WhereHas("Posts", func(q *Model[RelPost]) {
				if !leadingOr {
					q.Where("title", "missing")
				}
				q.OrWhere("title", "Post 1")
			}).Get(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(users) != 1 || users[0].ID != 1 {
				t.Fatalf("expected only parent 1, got %v", users)
			}
		})
	}
}

func TestWithCallback_OrKeepsMorphType(t *testing.T) {
	for _, limit := range []int{0, 1} {
		t.Run(map[int]string{0: "unlimited", 1: "limited"}[limit], func(t *testing.T) {
			db := setupMorphDB(t)
			posts, err := New[McPost]().SetDB(db).WithCallback("Images", func(q *Model[McImage]) {
				q.Where("url", "post.jpg").OrWhere("url", "avatar.jpg").OrderBy("id", "DESC").Limit(limit)
			}).Get(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(posts) != 1 {
				t.Fatalf("expected one post, got %d", len(posts))
			}
			if len(posts[0].Images) != 1 || posts[0].Images[0].ImageableType != "posts" {
				t.Fatalf("expected only the post's image, got %+v", posts[0].Images)
			}
		})
	}
}
