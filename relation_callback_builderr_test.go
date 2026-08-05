package zorm

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// extractRelationConstraints reads a WithCallback callback's state through the
// exported getters — GetWheres, GetArgs, GetOrderBys, GetLimit — and there was
// no getter for buildErr, so a rejected condition inside the callback vanished
// and the eager load returned EVERY child instead of the filtered set. Same
// failure mode as the nested Where(func) group fixed in N3.

type RcAuthor struct {
	ID    int64  `zorm:"primaryKey"`
	Name  string `zorm:"column:name"`
	Books []*RcBook
}

func (RcAuthor) TableName() string { return "rc_authors" }

func (RcAuthor) BooksRelation() HasMany[RcBook] {
	return HasMany[RcBook]{ForeignKey: "author_id"}
}

type RcBook struct {
	ID       int64  `zorm:"primaryKey"`
	AuthorID int64  `zorm:"column:author_id"`
	Title    string `zorm:"column:title"`
}

func (RcBook) TableName() string { return "rc_books" }

func setupRelCallbackDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`
		CREATE TABLE rc_authors (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE rc_books (id INTEGER PRIMARY KEY, author_id INTEGER, title TEXT);
		INSERT INTO rc_authors (id, name) VALUES (1, 'A');
		INSERT INTO rc_books (id, author_id, title) VALUES (1,1,'published'), (2,1,'draft');
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

// TestWithCallback_SurfacesInvalidColumn is the headline: a rejected filter
// inside the callback must not quietly widen the relation.
func TestWithCallback_SurfacesInvalidColumn(t *testing.T) {
	db := setupRelCallbackDB(t)
	ctx := context.Background()

	authors, err := New[RcAuthor]().SetDB(db).
		WithCallback("Books", func(q *Model[RcBook]) {
			q.Where("title; DROP TABLE rc_books", "published")
		}).
		Get(ctx)

	if !errors.Is(err, ErrInvalidColumnName) {
		t.Fatalf("expected ErrInvalidColumnName, got %v", err)
	}
	if len(authors) > 0 && len(authors[0].Books) > 0 {
		t.Errorf("expected no children loaded on a rejected callback, got %d", len(authors[0].Books))
	}
}

// TestWithCallback_SurfacesInvalidOperator covers the operator whitelist.
func TestWithCallback_SurfacesInvalidOperator(t *testing.T) {
	db := setupRelCallbackDB(t)
	ctx := context.Background()

	_, err := New[RcAuthor]().SetDB(db).
		WithCallback("Books", func(q *Model[RcBook]) {
			q.Where("title", "BOGUS_OP", "published")
		}).
		Get(ctx)

	if err == nil {
		t.Fatal("expected the callback's build error to surface")
	}
}

// TestWithCallback_SurfacesInvalidOrderDirection covers the case this became
// reachable through: review item #15 turned an unrecognized ORDER BY direction
// from silent coercion into a buildErr, which this path then ate — silently
// dropping the ordering AND changing which rows a per-parent Limit keeps.
func TestWithCallback_SurfacesInvalidOrderDirection(t *testing.T) {
	db := setupRelCallbackDB(t)
	ctx := context.Background()

	_, err := New[RcAuthor]().SetDB(db).
		WithCallback("Books", func(q *Model[RcBook]) {
			q.OrderBy("id", "ASCENDING").Limit(1)
		}).
		Get(ctx)

	if !errors.Is(err, ErrInvalidColumnName) {
		t.Fatalf("expected ErrInvalidColumnName for the bad direction, got %v", err)
	}
}

// TestLoad_SurfacesCallbackBuildErr covers the lazy-loading entry point.
func TestLoad_SurfacesCallbackBuildErr(t *testing.T) {
	db := setupRelCallbackDB(t)
	ctx := context.Background()

	author, err := New[RcAuthor]().SetDB(db).Find(ctx, 1)
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	err = New[RcAuthor]().SetDB(db).
		WithCallback("Books", func(q *Model[RcBook]) {
			q.Where("title; DROP TABLE rc_books", "published")
		}).
		Load(ctx, author, "Books")
	if !errors.Is(err, ErrInvalidColumnName) {
		t.Fatalf("expected ErrInvalidColumnName, got %v", err)
	}
}

// TestWithCallback_ValidCallbackUnaffected guards against overcorrecting.
func TestWithCallback_ValidCallbackUnaffected(t *testing.T) {
	db := setupRelCallbackDB(t)
	ctx := context.Background()

	authors, err := New[RcAuthor]().SetDB(db).
		WithCallback("Books", func(q *Model[RcBook]) {
			q.Where("title", "published").OrderBy("id", "ASC")
		}).
		Get(ctx)
	if err != nil {
		t.Fatalf("valid callback failed: %v", err)
	}
	if len(authors) != 1 || len(authors[0].Books) != 1 {
		t.Fatalf("expected 1 author with 1 filtered book, got %d authors", len(authors))
	}
	if authors[0].Books[0].Title != "published" {
		t.Errorf("expected the filter to apply, got %q", authors[0].Books[0].Title)
	}
}
