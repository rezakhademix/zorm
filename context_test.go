package zorm

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestContextCancellation(t *testing.T) {
	// Setup DB
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert a record
	_, err = db.Exec(`INSERT INTO users (name) VALUES ('Alice')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	orm := New[struct {
		ID   int64  `zorm:"column:id;primary;auto"`
		Name string `zorm:"column:name"`
	}]().SetDB(db)

	// Create a context that is already canceled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Try to find
	_, err = orm.Find(ctx, 1)
	if err == nil {
		t.Error("Expected error due to canceled context, got nil")
	} else if err != context.Canceled {
		// sql package might return its own error wrapping context.Canceled?
		// usually it returns context.Canceled directly or wrapped.
		// Let's check if it contains "canceled"
		if err != context.Canceled && err.Error() != "context canceled" {
			t.Logf("Got error: %v", err)
		}
	}
}

func TestContextTimeout(t *testing.T) {
	// Setup DB
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	orm := New[struct {
		ID   int64  `zorm:"column:id;primary;auto"`
		Name string `zorm:"column:name"`
	}]().SetDB(db)

	// Create a context with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	time.Sleep(1 * time.Millisecond) // Ensure timeout expires

	// Try to find
	_, err = orm.Find(ctx, 1)
	if err == nil {
		t.Error("Expected error due to timeout, got nil")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

type ContextUser struct {
	ID    int64  `zorm:"column:id;primary;auto"`
	Name  string `zorm:"column:name"`
	Posts []ContextPost
}

func (u ContextUser) PostsRelation() HasMany[ContextPost] {
	return HasMany[ContextPost]{ForeignKey: "user_id"}
}

type ContextPost struct {
	ID     int64  `zorm:"column:id;primary;auto"`
	UserID int64  `zorm:"column:user_id"`
	Title  string `zorm:"column:title"`
}

func TestLoadContext(t *testing.T) {
	// Setup DB
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Exec(`CREATE TABLE context_users (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}
	_, err = db.Exec(`CREATE TABLE context_posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create posts table: %v", err)
	}

	// Insert data
	_, err = db.Exec(`INSERT INTO context_users (name) VALUES ('Bob')`)
	if err != nil {
		t.Fatalf("Failed to insert user: %v", err)
	}
	_, err = db.Exec(`INSERT INTO context_posts (user_id, title) VALUES (1, 'Post 1')`)
	if err != nil {
		t.Fatalf("Failed to insert post: %v", err)
	}

	orm := New[ContextUser]().SetDB(db)
	user, err := orm.Find(context.Background(), 1)
	if err != nil {
		t.Fatalf("Failed to find user: %v", err)
	}

	// Test Load with cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = orm.Load(ctx, user, "Posts")
	if err == nil {
		t.Error("Expected error from Load with cancelled context, got nil")
	} else if err != context.Canceled && err.Error() != "context canceled" {
		t.Logf("Got error: %v", err)
	}
}
