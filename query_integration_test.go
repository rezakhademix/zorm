package zorm

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

type QUser struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Email string
}

func (u QUser) TableName() string { return "q_users" }

func setupQueryDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE q_users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);
		INSERT INTO q_users (name, email) VALUES 
		('User 1', 'u1@example.com'),
		('User 2', 'u2@example.com'),
		('User 3', 'u3@example.com'),
		('User 4', 'u4@example.com'),
		('User 5', 'u5@example.com');
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

func TestQuery_Pagination(t *testing.T) {
	db := setupQueryDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	// Test Paginate
	res, err := New[QUser]().OrderBy("id", "ASC").Paginate(ctx, 1, 2)
	if err != nil {
		t.Fatalf("Paginate failed: %v", err)
	}

	if res.Total != 5 {
		t.Errorf("expected total 5, got %d", res.Total)
	}
	if len(res.Data) != 2 {
		t.Errorf("expected 2 items on page 1, got %d", len(res.Data))
	}

	// Test SimplePaginate
	res, err = New[QUser]().OrderBy("id", "ASC").SimplePaginate(ctx, 1, 2)
	if err != nil {
		t.Fatalf("SimplePaginate failed: %v", err)
	}
	if res.Total != -1 {
		t.Errorf("SimplePaginate should return Total -1, got %d", res.Total)
	}
}

func TestQuery_Chunk(t *testing.T) {
	db := setupQueryDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	count := 0
	err := New[QUser]().OrderBy("id", "ASC").Chunk(ctx, 2, func(users []*QUser) error {
		count += len(users)
		return nil
	})

	if err != nil {
		t.Fatalf("Chunk failed: %v", err)
	}
	if count != 5 {
		t.Errorf("expected 5 users processed, got %d", count)
	}
}

type QUserWithPosts struct {
	ID   int `zorm:"primaryKey"`
	Name string
}

func (u QUserWithPosts) TableName() string { return "q_users" }
func (u QUserWithPosts) PostsRelation() HasMany[RelPost] {
	return HasMany[RelPost]{
		ForeignKey: "user_id",
		Table:      "rel_posts",
	}
}

func TestQuery_WhereHas_Execution(t *testing.T) {
	db := setupQueryDB(t)
	defer db.Close()

	_, _ = db.Exec(`
		CREATE TABLE rel_posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);
		INSERT INTO rel_posts (user_id, title) VALUES (1, 'Post 1');
	`)

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	// Should only find User 1 who has a post
	q := New[QUserWithPosts]().WhereHas("Posts", nil)
	sqlStr, args := q.Print()
	t.Logf("SQL: %s, Args: %v", sqlStr, args)

	users, err := q.Get(ctx)
	if err != nil {
		t.Fatalf("WhereHas failed: %v", err)
	}

	if len(users) != 1 {
		t.Errorf("expected 1 user, got %d", len(users))
	} else if users[0].Name != "User 1" {
		t.Errorf("expected User 1, got %s", users[0].Name)
	}

	// With constraint
	users, _ = New[QUserWithPosts]().WhereHas("Posts", func(q *Model[RelPost]) {
		q.Where("title", "Other")
	}).Get(ctx)

	if len(users) != 0 {
		t.Errorf("expected 0 users with Post 'Other', got %d", len(users))
	}
}
func TestQuery_Security(t *testing.T) {
	db := setupQueryDB(t)
	defer db.Close()

	ctx := context.Background()
	// Malicious key in UpdateMany
	err := New[QUser]().SetDB(db).UpdateMany(ctx, map[string]any{
		"name = 'hacked'; --": "malicious",
	})
	if err == nil {
		t.Error("expected error for malicious key in UpdateMany")
	}
}

func TestQuery_CreateMany_Batching(t *testing.T) {
	db := setupQueryDB(t)
	defer db.Close()

	ctx := context.Background()
	// Create 505 users to trigger chunking (batchSize=500)
	users := make([]*QUser, 505)
	for i := 0; i < 505; i++ {
		users[i] = &QUser{Name: fmt.Sprintf("User %d", i), Email: fmt.Sprintf("u%d@example.com", i)}
	}

	err := New[QUser]().SetDB(db).CreateMany(ctx, users)
	if err != nil {
		t.Fatalf("CreateMany batching failed: %v", err)
	}

	// Verify count
	var count int64
	_ = db.QueryRow("SELECT COUNT(*) FROM q_users").Scan(&count)
	if count != 510 { // 5 initial + 505 new
		t.Errorf("expected 510 users total, got %d", count)
	}

	// Verify ID hydration of last item
	if users[504].ID == 0 {
		t.Error("expected last user ID to be hydrated")
	}
}
