package zorm

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// The morph API had two conventions living side by side: MorphOne/MorphMany
// treat Type/ID as DB column names while MorphTo resolved them as struct field
// names, and the value written into the type column was the Go struct name
// while the documented examples used table names. These tests pin one
// convention: Type/ID are DB column names everywhere, and the morph type value
// comes from a single source of truth (MorphType() when declared, struct name
// otherwise) shared by every morph loader.

// McPost declares MorphType(), so its rows are tagged "posts" in the type column.
type McPost struct {
	ID     int64  `zorm:"primaryKey"`
	Title  string `zorm:"column:title"`
	Images []*McImage
}

func (McPost) TableName() string { return "mc_posts" }
func (McPost) MorphType() string { return "posts" }

func (McPost) ImagesRelation() MorphMany[McImage] {
	return MorphMany[McImage]{Type: "imageable_type", ID: "imageable_id"}
}

// McUser has no MorphType(), so it keeps the default: its Go struct name.
type McUser struct {
	ID     int64  `zorm:"primaryKey"`
	Name   string `zorm:"column:name"`
	Avatar *McImage
}

func (McUser) TableName() string { return "mc_users" }

func (McUser) AvatarRelation() MorphOne[McImage] {
	return MorphOne[McImage]{Type: "imageable_type", ID: "imageable_id"}
}

// McImage is the morph child. Its MorphTo config uses DB column names, which is
// what the field documentation and the README have always described.
type McImage struct {
	ID            int64  `zorm:"primaryKey"`
	URL           string `zorm:"column:url"`
	ImageableID   int64  `zorm:"column:imageable_id"`
	ImageableType string `zorm:"column:imageable_type"`
	Imageable     any
}

func (McImage) TableName() string { return "mc_images" }

func (McImage) ImageableRelation() MorphTo[any] {
	return MorphTo[any]{
		Type: "imageable_type",
		ID:   "imageable_id",
		TypeMap: map[string]any{
			"posts":  McPost{},
			"McUser": McUser{},
		},
	}
}

// McImageFieldNames keeps the legacy struct-field-name form of the MorphTo
// config, which must keep working.
type McImageFieldNames struct {
	ID            int64  `zorm:"primaryKey"`
	URL           string `zorm:"column:url"`
	ImageableID   int64  `zorm:"column:imageable_id"`
	ImageableType string `zorm:"column:imageable_type"`
	Imageable     any
}

func (McImageFieldNames) TableName() string { return "mc_images" }

func (McImageFieldNames) ImageableRelation() MorphTo[any] {
	return MorphTo[any]{
		Type: "ImageableType",
		ID:   "ImageableID",
		TypeMap: map[string]any{
			"posts":  McPost{},
			"McUser": McUser{},
		},
	}
}

func setupMorphDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`
		CREATE TABLE mc_posts (id INTEGER PRIMARY KEY, title TEXT);
		CREATE TABLE mc_users (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE mc_images (
			id INTEGER PRIMARY KEY,
			url TEXT,
			imageable_id INTEGER,
			imageable_type TEXT
		);

		INSERT INTO mc_posts (id, title) VALUES (1, 'Hello');
		INSERT INTO mc_users (id, name) VALUES (1, 'Alice');
		INSERT INTO mc_images (id, url, imageable_id, imageable_type) VALUES
			(1, 'post.jpg',   1, 'posts'),
			(2, 'avatar.jpg', 1, 'McUser');
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

// TestMorph_TypeValueRespectsMorphType covers the type value written by
// MorphMany's lookup: with MorphType() declared it must match the rows the
// model actually tags, not the Go struct name.
func TestMorph_TypeValueRespectsMorphType(t *testing.T) {
	db := setupMorphDB(t)
	ctx := context.Background()

	posts, err := New[McPost]().SetDB(db).With("Images").Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(posts) != 1 {
		t.Fatalf("expected 1 post, got %d", len(posts))
	}
	if len(posts[0].Images) != 1 {
		t.Fatalf("expected the post's image to load via MorphType() %q, got %d images", "posts", len(posts[0].Images))
	}
	if posts[0].Images[0].URL != "post.jpg" {
		t.Errorf("expected post.jpg, got %q", posts[0].Images[0].URL)
	}
}

// TestMorph_TypeValueDefaultsToStructName guards the default for models that do
// not declare MorphType().
func TestMorph_TypeValueDefaultsToStructName(t *testing.T) {
	db := setupMorphDB(t)
	ctx := context.Background()

	users, err := New[McUser]().SetDB(db).With("Avatar").Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(users) != 1 {
		t.Fatalf("expected 1 user, got %d", len(users))
	}
	if users[0].Avatar == nil {
		t.Fatal("expected the user's avatar to load")
	}
	if users[0].Avatar.URL != "avatar.jpg" {
		t.Errorf("expected avatar.jpg, got %q", users[0].Avatar.URL)
	}
}

// TestMorphTo_AcceptsColumnNames is the API-consistency regression: MorphTo's
// Type/ID are documented as column names (as they are for MorphOne/MorphMany),
// but the loader looked them up as struct field names and silently loaded
// nothing when given the documented form.
func TestMorphTo_AcceptsColumnNames(t *testing.T) {
	db := setupMorphDB(t)
	ctx := context.Background()

	images, err := New[McImage]().SetDB(db).With("Imageable").OrderBy("id", "ASC").Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(images) != 2 {
		t.Fatalf("expected 2 images, got %d", len(images))
	}

	post, ok := images[0].Imageable.(*McPost)
	if !ok {
		t.Fatalf("expected image 1 to resolve to *McPost, got %T", images[0].Imageable)
	}
	if post.Title != "Hello" {
		t.Errorf("expected post title Hello, got %q", post.Title)
	}

	user, ok := images[1].Imageable.(*McUser)
	if !ok {
		t.Fatalf("expected image 2 to resolve to *McUser, got %T", images[1].Imageable)
	}
	if user.Name != "Alice" {
		t.Errorf("expected user Alice, got %q", user.Name)
	}
}

// TestMorphTo_AcceptsStructFieldNames pins backward compatibility for configs
// written against the old behavior.
func TestMorphTo_AcceptsStructFieldNames(t *testing.T) {
	db := setupMorphDB(t)
	ctx := context.Background()

	images, err := New[McImageFieldNames]().SetDB(db).With("Imageable").OrderBy("id", "ASC").Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(images) != 2 {
		t.Fatalf("expected 2 images, got %d", len(images))
	}
	if _, ok := images[0].Imageable.(*McPost); !ok {
		t.Errorf("expected image 1 to resolve to *McPost, got %T", images[0].Imageable)
	}
	if _, ok := images[1].Imageable.(*McUser); !ok {
		t.Errorf("expected image 2 to resolve to *McUser, got %T", images[1].Imageable)
	}
}

// McWideImage stores the morph id in a narrower Go type than the parent's
// primary key, which used to break the raw map[any] key matching in loadMorphTo
// (int32 key never equals an int64 key).
type McWideImage struct {
	ID            int64  `zorm:"primaryKey"`
	URL           string `zorm:"column:url"`
	ImageableID   int32  `zorm:"column:imageable_id"`
	ImageableType string `zorm:"column:imageable_type"`
	Imageable     any
}

func (McWideImage) TableName() string { return "mc_images" }

func (McWideImage) ImageableRelation() MorphTo[any] {
	return MorphTo[any]{
		Type:    "imageable_type",
		ID:      "imageable_id",
		TypeMap: map[string]any{"posts": McPost{}},
	}
}

// TestMorphTo_MatchesAcrossIntWidths covers the key normalization the other
// relation loaders already use.
func TestMorphTo_MatchesAcrossIntWidths(t *testing.T) {
	db := setupMorphDB(t)
	ctx := context.Background()

	images, err := New[McWideImage]().SetDB(db).With("Imageable").Where("id", 1).Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(images) != 1 {
		t.Fatalf("expected 1 image, got %d", len(images))
	}
	if _, ok := images[0].Imageable.(*McPost); !ok {
		t.Errorf("expected the int32 morph id to match the int64 parent key, got %T", images[0].Imageable)
	}
}

// TestMorphTo_LoadsAllTypesOnOneConnection guards the loadMorphTo refactor: the
// per-type query block used to be inlined in the loop with its own deferred
// rows.Close(), so every morph type held a cursor until the function returned.
// Pinning the pool to one connection makes any real cursor leak fatal.
func TestMorphTo_LoadsAllTypesOnOneConnection(t *testing.T) {
	db := setupMorphDB(t) // already SetMaxOpenConns(1)
	ctx := context.Background()

	images, err := New[McImage]().SetDB(db).With("Imageable").OrderBy("id", "ASC").Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(images) != 2 {
		t.Fatalf("expected 2 images, got %d", len(images))
	}
	if _, ok := images[0].Imageable.(*McPost); !ok {
		t.Errorf("expected image 1 to resolve to *McPost, got %T", images[0].Imageable)
	}
	if _, ok := images[1].Imageable.(*McUser); !ok {
		t.Errorf("expected image 2 to resolve to *McUser, got %T", images[1].Imageable)
	}

	// The connection must be free for ordinary use right afterwards.
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM mc_images`).Scan(&n); err != nil {
		t.Fatalf("connection was not released: %v", err)
	}
}
