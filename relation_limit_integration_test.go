package zorm

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// A Limit inside WithCallback used to apply to the eager-load query as a whole,
// so "5 posts per user" actually meant "5 posts across all users" — the first
// user got everything and the rest got nothing. These tests pin the per-parent
// meaning the README documents.

type RlUser struct {
	ID     int64  `zorm:"primaryKey"`
	Name   string `zorm:"column:name"`
	Posts  []*RlPost
	Photos []*RlPhoto
}

func (RlUser) TableName() string { return "rl_users" }

func (RlUser) PostsRelation() HasMany[RlPost] {
	return HasMany[RlPost]{ForeignKey: "user_id"}
}

func (RlUser) PhotosRelation() MorphMany[RlPhoto] {
	return MorphMany[RlPhoto]{Type: "imageable_type", ID: "imageable_id"}
}

type RlPost struct {
	ID     int64  `zorm:"primaryKey"`
	UserID int64  `zorm:"column:user_id"`
	Title  string `zorm:"column:title"`
}

func (RlPost) TableName() string { return "rl_posts" }

type RlPhoto struct {
	ID            int64  `zorm:"primaryKey"`
	ImageableID   int64  `zorm:"column:imageable_id"`
	ImageableType string `zorm:"column:imageable_type"`
	URL           string `zorm:"column:url"`
}

func (RlPhoto) TableName() string { return "rl_photos" }

func setupRelationLimitDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`
		CREATE TABLE rl_users (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE rl_posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);
		CREATE TABLE rl_photos (
			id INTEGER PRIMARY KEY,
			imageable_id INTEGER,
			imageable_type TEXT,
			url TEXT
		);

		INSERT INTO rl_users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		INSERT INTO rl_posts (id, user_id, title) VALUES
			(1, 1, 'alice-1'), (2, 1, 'alice-2'), (3, 1, 'alice-3'),
			(4, 2, 'bob-1'),   (5, 2, 'bob-2');
		INSERT INTO rl_photos (id, imageable_id, imageable_type, url) VALUES
			(1, 1, 'RlUser', 'alice-a.jpg'), (2, 1, 'RlUser', 'alice-b.jpg'),
			(3, 2, 'RlUser', 'bob-a.jpg'),   (4, 2, 'RlUser', 'bob-b.jpg');
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

// TestWithCallback_LimitIsPerParent covers HasMany: every parent must get up to
// `limit` children, not the first parent taking the whole budget.
func TestWithCallback_LimitIsPerParent(t *testing.T) {
	db := setupRelationLimitDB(t)
	ctx := context.Background()

	users, err := New[RlUser]().SetDB(db).
		WithCallback("Posts", func(q *Model[RlPost]) {
			q.OrderBy("id", "ASC").Limit(2)
		}).
		OrderBy("id", "ASC").
		Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}

	for _, u := range users {
		if len(u.Posts) != 2 {
			t.Errorf("user %d: expected 2 posts (per-parent limit), got %d", u.ID, len(u.Posts))
		}
	}
	if len(users[0].Posts) == 2 && users[0].Posts[0].Title != "alice-1" {
		t.Errorf("expected the partition to respect ORDER BY id ASC, got %q", users[0].Posts[0].Title)
	}
}

// TestWithCallback_LimitRespectsOrderWithinParent verifies the ordering used to
// pick which children survive the per-parent limit.
func TestWithCallback_LimitRespectsOrderWithinParent(t *testing.T) {
	db := setupRelationLimitDB(t)
	ctx := context.Background()

	users, err := New[RlUser]().SetDB(db).
		WithCallback("Posts", func(q *Model[RlPost]) {
			q.OrderBy("id", "DESC").Limit(1)
		}).
		OrderBy("id", "ASC").
		Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	want := map[int64]string{1: "alice-3", 2: "bob-2"}
	for _, u := range users {
		if len(u.Posts) != 1 {
			t.Fatalf("user %d: expected 1 post, got %d", u.ID, len(u.Posts))
		}
		if u.Posts[0].Title != want[u.ID] {
			t.Errorf("user %d: expected %q, got %q", u.ID, want[u.ID], u.Posts[0].Title)
		}
	}
}

// TestWithCallback_LimitWithWhereIsPerParent verifies the limit is applied after
// the callback's own filters, per parent.
func TestWithCallback_LimitWithWhereIsPerParent(t *testing.T) {
	db := setupRelationLimitDB(t)
	ctx := context.Background()

	users, err := New[RlUser]().SetDB(db).
		WithCallback("Posts", func(q *Model[RlPost]) {
			q.Where("id", ">", 1).OrderBy("id", "ASC").Limit(1)
		}).
		OrderBy("id", "ASC").
		Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	want := map[int64]string{1: "alice-2", 2: "bob-1"}
	for _, u := range users {
		if len(u.Posts) != 1 {
			t.Fatalf("user %d: expected 1 post, got %d", u.ID, len(u.Posts))
		}
		if u.Posts[0].Title != want[u.ID] {
			t.Errorf("user %d: expected %q, got %q", u.ID, want[u.ID], u.Posts[0].Title)
		}
	}
}

// TestWithCallback_LimitIsPerParentForMorphMany covers the morph loader, which
// builds its own SELECT and applied the limit the same global way.
func TestWithCallback_LimitIsPerParentForMorphMany(t *testing.T) {
	db := setupRelationLimitDB(t)
	ctx := context.Background()

	users, err := New[RlUser]().SetDB(db).
		WithCallback("Photos", func(q *Model[RlPhoto]) {
			q.OrderBy("id", "ASC").Limit(1)
		}).
		OrderBy("id", "ASC").
		Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	want := map[int64]string{1: "alice-a.jpg", 2: "bob-a.jpg"}
	for _, u := range users {
		if len(u.Photos) != 1 {
			t.Fatalf("user %d: expected 1 photo, got %d", u.ID, len(u.Photos))
		}
		if u.Photos[0].URL != want[u.ID] {
			t.Errorf("user %d: expected %q, got %q", u.ID, want[u.ID], u.Photos[0].URL)
		}
	}
}

// TestWithCallback_NoLimitUnaffected guards against overcorrecting: without a
// limit every child still loads.
func TestWithCallback_NoLimitUnaffected(t *testing.T) {
	db := setupRelationLimitDB(t)
	ctx := context.Background()

	users, err := New[RlUser]().SetDB(db).
		WithCallback("Posts", func(q *Model[RlPost]) {
			q.OrderBy("id", "ASC")
		}).
		OrderBy("id", "ASC").
		Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(users[0].Posts) != 3 || len(users[1].Posts) != 2 {
		t.Errorf("expected 3 and 2 posts, got %d and %d", len(users[0].Posts), len(users[1].Posts))
	}
}
