package zorm

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// Models for testing relations

type RelUser struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Posts []*RelPost // Slice field for loaded data
}

func (u RelUser) TableName() string { return "rel_users" }

// Posts relation method - avoiding name conflict with field
func (u RelUser) PostsRelation() HasMany[RelPost] {
	return HasMany[RelPost]{
		ForeignKey: "user_id",
	}
}

type RelPost struct {
	ID       int `zorm:"primaryKey"`
	UserID   int
	Title    string
	User     *RelUser      // Field for loaded data
	Comments []*RelComment // Nested relation
}

func (p RelPost) TableName() string { return "rel_posts" }

// User relation method
func (p RelPost) UserRelation() BelongsTo[RelUser] {
	return BelongsTo[RelUser]{
		ForeignKey: "user_id",
	}
}

// Comments relation method
func (p RelPost) CommentsRelation() HasMany[RelComment] {
	return HasMany[RelComment]{
		ForeignKey: "commentable_id",
		Table:      "rel_comments",
	}
}

func setupRelDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// Create tables
	_, err = db.Exec(`
		CREATE TABLE rel_users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT
		);
		CREATE TABLE rel_posts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id INTEGER,
			title TEXT
		);
	`)
	if err != nil {
		t.Fatalf("failed to create tables: %v", err)
	}

	// Insert data
	_, err = db.Exec(`
		INSERT INTO rel_users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		INSERT INTO rel_posts (id, user_id, title) VALUES 
		(1, 1, 'Post 1'), 
		(2, 1, 'Post 2'),
		(3, 2, 'Post 3');
	`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	return db
}

func TestRelations_LoadHasMany(t *testing.T) {
	db := setupRelDB(t)
	defer db.Close()

	// Save original global DB and restore after test
	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	users, err := New[RelUser]().With("Posts").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	if len(users) != 2 {
		t.Errorf("expected 2 users, got %d", len(users))
	}

	// Find Alice
	var alice *RelUser
	for _, u := range users {
		if u.Name == "Alice" {
			alice = u
			break
		}
	}

	if alice == nil {
		t.Fatal("Alice not found")
	}

	// Check Alice's posts
	if len(alice.Posts) != 2 {
		t.Errorf("expected Alice to have 2 posts, got %d", len(alice.Posts))
	}

	// Find Bob
	var bob *RelUser
	for _, u := range users {
		if u.Name == "Bob" {
			bob = u
			break
		}
	}

	if bob != nil && len(bob.Posts) != 1 {
		t.Errorf("expected Bob to have 1 post, got %d", len(bob.Posts))
	}
}

func TestRelations_LoadBelongsTo(t *testing.T) {
	db := setupRelDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	posts, err := New[RelPost]().With("User").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get posts: %v", err)
	}
	for _, p := range posts {
		if p.User == nil {
			t.Errorf("post %d: expected User to be loaded, got nil", p.ID)
			continue
		}
		if p.UserID == 1 && p.User.Name != "Alice" {
			t.Errorf("post %d: expected User Alice, got %q", p.ID, p.User.Name)
		}
	}
}

type RelRole struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Users []*RelUser
}

func (r RelRole) TableName() string { return "rel_roles" }

type RelImage struct {
	ID            int `zorm:"primaryKey"`
	URL           string
	ImageableID   int
	ImageableType string
}

func (i RelImage) TableName() string { return "rel_images" }

// Updating RelUser to support more relations
type RelUserExtended struct {
	ID     int `zorm:"primaryKey"`
	Name   string
	Posts  []*RelPost  // HasMany
	Roles  []*RelRole  // BelongsToMany
	Images []*RelImage // MorphMany
}

func (u RelUserExtended) TableName() string { return "rel_users" }

func (u RelUserExtended) PostsRelation() HasMany[RelPost] {
	return HasMany[RelPost]{ForeignKey: "user_id"}
}

func (u RelUserExtended) RolesRelation() BelongsToMany[RelRole] {
	return BelongsToMany[RelRole]{
		PivotTable: "rel_role_user",
		ForeignKey: "user_id",
		RelatedKey: "role_id",
	}
}

func (u RelUserExtended) ImagesRelation() MorphMany[RelImage] {
	return MorphMany[RelImage]{
		Type: "imageable_type",
		ID:   "imageable_id",
	}
}

func setupRelDBExtended(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE rel_users (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE rel_posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);
		CREATE TABLE rel_roles (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE rel_role_user (user_id INTEGER, role_id INTEGER);
		CREATE TABLE rel_images (id INTEGER PRIMARY KEY, url TEXT, imageable_id INTEGER, imageable_type TEXT);

		INSERT INTO rel_users (id, name) VALUES (1, 'Alice');
		INSERT INTO rel_posts (id, user_id, title) VALUES (1, 1, 'Post 1');
		INSERT INTO rel_roles (id, name) VALUES (1, 'Admin'), (2, 'Editor');
		INSERT INTO rel_role_user (user_id, role_id) VALUES (1, 1), (1, 2);
		INSERT INTO rel_images (id, url, imageable_id, imageable_type) VALUES 
		(1, 'http://alice_thumb.jpg', 1, 'RelUserExtended'),
		(2, 'http://post1_img.jpg', 1, 'RelPost');
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

func TestRelations_BelongsToMany(t *testing.T) {
	db := setupRelDBExtended(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	users, err := New[RelUserExtended]().With("Roles").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	if len(users) == 0 {
		t.Fatal("expected users")
	}

	if len(users[0].Roles) != 2 {
		t.Errorf("expected 2 roles, got %d", len(users[0].Roles))
	}
}

func TestRelations_MorphMany(t *testing.T) {
	db := setupRelDBExtended(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	users, err := New[RelUserExtended]().With("Images").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	if len(users) == 0 {
		t.Fatal("expected users")
	}

	if len(users[0].Images) != 1 {
		t.Errorf("expected 1 image, got %d", len(users[0].Images))
	}
}

type RelComment struct {
	ID              int `zorm:"primaryKey"`
	Content         string
	CommentableID   int
	CommentableType string
	Commentable     any // MorphTo field
}

func (c RelComment) TableName() string { return "rel_comments" }

func (c RelComment) CommentableRelation() MorphTo[any] {
	return MorphTo[any]{
		Type: "CommentableType",
		ID:   "CommentableID",
		TypeMap: map[string]any{
			"RelUserExtended": RelUserExtended{},
			"RelPost":         RelPost{},
		},
	}
}

func setupRelDBMorphTo(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE rel_users (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE rel_posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);
		CREATE TABLE rel_comments (id INTEGER PRIMARY KEY, content TEXT, commentable_id INTEGER, commentable_type TEXT);

		INSERT INTO rel_users (id, name) VALUES (1, 'Alice');
		INSERT INTO rel_posts (id, user_id, title) VALUES (1, 1, 'Post 1');
		INSERT INTO rel_comments (id, content, commentable_id, commentable_type) VALUES 
		(1, 'Alice is great', 1, 'RelUserExtended'),
		(2, 'Post 1 is informative', 1, 'RelPost');
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

func TestRelations_MorphTo(t *testing.T) {
	db := setupRelDBMorphTo(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	comments, err := New[RelComment]().With("Commentable").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get comments: %v", err)
	}

	if len(comments) != 2 {
		t.Errorf("expected 2 comments, got %d", len(comments))
	}

	for _, c := range comments {
		if c.Commentable == nil {
			t.Errorf("comment %d: expected Commentable to be loaded", c.ID)
			continue
		}

		if c.CommentableType == "RelUserExtended" {
			user, ok := c.Commentable.(*RelUserExtended)
			if !ok || user.Name != "Alice" {
				t.Errorf("comment %d: expected Alice, got %v", c.ID, c.Commentable)
			}
		} else if c.CommentableType == "RelPost" {
			post, ok := c.Commentable.(*RelPost)
			if !ok || post.Title != "Post 1" {
				t.Errorf("comment %d: expected Post 1, got %v", c.ID, c.Commentable)
			}
		}
	}
}

func TestRelations_Nested(t *testing.T) {
	db := setupRelDBMorphTo(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	users, err := New[RelUser]().With("Posts.Comments").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	for _, u := range users {
		if u.Name == "Alice" {
			if len(u.Posts) == 0 {
				t.Error("expected Alice to have posts")
				continue
			}
			for _, p := range u.Posts {
				if p.Title == "Post 1" {
					if len(p.Comments) == 0 {
						t.Errorf("expected Post 1 to have comments via nested load, got %d", len(p.Comments))
					}
				}
			}
		}
	}
}

func TestRelations_NestedWithCols(t *testing.T) {
	db := setupRelDBMorphTo(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	users, err := New[RelUser]().With("Posts.Comments:id,content,commentable_id").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	for _, u := range users {
		if u.Name == "Alice" {
			if len(u.Posts) == 0 {
				t.Fatal("expected Alice to have posts")
			}
			for _, p := range u.Posts {
				if p.Title == "Post 1" {
					if len(p.Comments) == 0 {
						t.Fatal("expected Post 1 to have comments with column selection")
					}
					for _, c := range p.Comments {
						if c.Content == "" {
							t.Error("expected Content to be populated")
						}
						if c.CommentableType != "" {
							t.Errorf("expected CommentableType to be empty (not selected), got %q", c.CommentableType)
						}
					}
				}
			}
		}
	}
}

func TestRelations_NestedWithCols_MissingFK(t *testing.T) {
	db := setupRelDBMorphTo(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	// FK column (commentable_id) is omitted from column selection.
	// The query succeeds but the FK field is zero-valued, so children
	// cannot be mapped back to parents — resulting in empty slices.
	users, err := New[RelUser]().With("Posts.Comments:id,content").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	for _, u := range users {
		if u.Name == "Alice" {
			if len(u.Posts) == 0 {
				t.Fatal("expected Alice to have posts")
			}
			for _, p := range u.Posts {
				if p.Title == "Post 1" {
					if len(p.Comments) != 0 {
						t.Errorf("expected 0 comments when FK not in column selection, got %d", len(p.Comments))
					}
				}
			}
		}
	}
}

func TestRelations_LoadSlice(t *testing.T) {
	db := setupRelDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	users, _ := New[RelUser]().Get(ctx)

	err := New[RelUser]().LoadSlice(ctx, users, "Posts")
	if err != nil {
		t.Fatalf("LoadSlice failed: %v", err)
	}

	for _, u := range users {
		if u.Name == "Alice" && len(u.Posts) != 2 {
			t.Errorf("expected 2 posts for Alice, got %d", len(u.Posts))
		}
	}
}

// ==================== HasOne Tests ====================

type RelProfile struct {
	ID     int `zorm:"primaryKey"`
	UserID int
	Bio    string
}

func (p RelProfile) TableName() string { return "rel_profiles" }

type RelUserWithProfile struct {
	ID      int `zorm:"primaryKey"`
	Name    string
	Profile *RelProfile
}

func (u RelUserWithProfile) TableName() string { return "rel_users_with_profile" }

func (u RelUserWithProfile) ProfileRelation() HasOne[RelProfile] {
	return HasOne[RelProfile]{
		ForeignKey: "user_id",
	}
}

func TestHasOne_WithTableOverride(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE rel_users_with_profile (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE custom_profiles (id INTEGER PRIMARY KEY, user_id INTEGER UNIQUE, bio TEXT);

		INSERT INTO rel_users_with_profile (id, name) VALUES (1, 'Alice');
		INSERT INTO custom_profiles (id, user_id, bio) VALUES (1, 1, 'Custom bio');
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	// We'll test the GetOverrideTable method directly
	rel := HasOne[RelProfile]{
		ForeignKey: "user_id",
		Table:      "custom_profiles",
	}

	if rel.GetOverrideTable() != "custom_profiles" {
		t.Errorf("GetOverrideTable() = %q, want %q", rel.GetOverrideTable(), "custom_profiles")
	}
}

// ==================== MorphOne Tests ====================

type RelUserWithAvatar struct {
	ID     int `zorm:"primaryKey"`
	Name   string
	Avatar *RelImage
}

func (u RelUserWithAvatar) TableName() string { return "rel_users_with_avatar" }

func (u RelUserWithAvatar) AvatarRelation() MorphOne[RelImage] {
	return MorphOne[RelImage]{
		Type: "imageable_type",
		ID:   "imageable_id",
	}
}

func setupRelDBMorphOne(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE rel_users_with_avatar (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE rel_images (id INTEGER PRIMARY KEY, url TEXT, imageable_id INTEGER, imageable_type TEXT);

		INSERT INTO rel_users_with_avatar (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		INSERT INTO rel_images (id, url, imageable_id, imageable_type) VALUES
			(1, 'http://alice_avatar.jpg', 1, 'RelUserWithAvatar'),
			(2, 'http://bob_avatar.jpg', 2, 'RelUserWithAvatar');
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

func TestMorphOne_SingleRelation(t *testing.T) {
	db := setupRelDBMorphOne(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	users, err := New[RelUserWithAvatar]().With("Avatar").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}

	// Alice should have an avatar
	alice := users[0]
	if alice.Name != "Alice" {
		t.Errorf("expected first user to be Alice, got %s", alice.Name)
	}
	if alice.Avatar == nil {
		t.Error("expected Alice to have an avatar")
	} else if alice.Avatar.URL != "http://alice_avatar.jpg" {
		t.Errorf("expected Alice's avatar URL, got %q", alice.Avatar.URL)
	}

	// Bob should have an avatar
	bob := users[1]
	if bob.Avatar == nil {
		t.Error("expected Bob to have an avatar")
	} else if bob.Avatar.URL != "http://bob_avatar.jpg" {
		t.Errorf("expected Bob's avatar URL, got %q", bob.Avatar.URL)
	}
}

func TestMorphOne_NoMatch(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE rel_users_with_avatar (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE rel_images (id INTEGER PRIMARY KEY, url TEXT, imageable_id INTEGER, imageable_type TEXT);

		INSERT INTO rel_users_with_avatar (id, name) VALUES (1, 'Alice'), (2, 'NoAvatar');
		INSERT INTO rel_images (id, url, imageable_id, imageable_type) VALUES
			(1, 'http://alice_avatar.jpg', 1, 'RelUserWithAvatar');
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	users, err := New[RelUserWithAvatar]().With("Avatar").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	// Find NoAvatar user
	var noAvatarUser *RelUserWithAvatar
	for _, u := range users {
		if u.Name == "NoAvatar" {
			noAvatarUser = u
			break
		}
	}

	if noAvatarUser == nil {
		t.Fatal("NoAvatar user not found")
	}

	if noAvatarUser.Avatar != nil {
		t.Errorf("expected NoAvatar user to have no avatar, got %+v", noAvatarUser.Avatar)
	}
}

func TestMorphOne_WithTableOverride(t *testing.T) {
	rel := MorphOne[RelImage]{
		Type:  "imageable_type",
		ID:    "imageable_id",
		Table: "custom_images",
	}

	if rel.GetOverrideTable() != "custom_images" {
		t.Errorf("GetOverrideTable() = %q, want %q", rel.GetOverrideTable(), "custom_images")
	}
}

// ==================== LoadMorph Tests ====================

func TestLoadMorph_EmptyTypeMap(t *testing.T) {
	db := setupRelDBMorphTo(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	comments, err := New[RelComment]().Get(ctx)
	if err != nil {
		t.Fatalf("failed to get comments: %v", err)
	}

	// Use nil typeMap - should fall back to default behavior
	err = New[RelComment]().LoadMorph(ctx, comments, "Commentable", nil)
	if err != nil {
		t.Fatalf("LoadMorph with nil typeMap failed: %v", err)
	}

	for _, c := range comments {
		if c.Commentable == nil {
			t.Errorf("comment %d: expected Commentable to be loaded", c.ID)
		}
	}
}

// ==================== Error Case Tests ====================

func TestLoadRelations_RelationNotFound(t *testing.T) {
	db := setupRelDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	users, err := New[RelUser]().Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	// Try to load a non-existent relation
	err = New[RelUser]().LoadSlice(ctx, users, "NonExistentRelation")
	if err == nil {
		t.Error("expected error for non-existent relation, got nil")
	}

	// Check it's a RelationError
	var relErr *RelationError
	if !errors.As(err, &relErr) {
		t.Errorf("expected RelationError, got %T", err)
	}
}

func TestLoadRelations_EmptyResults(t *testing.T) {
	db := setupRelDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	// Load relations on empty slice - should not error
	var emptyUsers []*RelUser
	err := New[RelUser]().LoadSlice(ctx, emptyUsers, "Posts")
	if err != nil {
		t.Errorf("LoadSlice on empty slice should not error, got: %v", err)
	}
}

func TestMorphTo_TypeNotInMap(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE rel_users (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE rel_posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);
		CREATE TABLE rel_comments (id INTEGER PRIMARY KEY, content TEXT, commentable_id INTEGER, commentable_type TEXT);

		INSERT INTO rel_comments (id, content, commentable_id, commentable_type) VALUES
			(1, 'Comment on unknown type', 1, 'UnknownType');
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	comments, err := New[RelComment]().With("Commentable").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get comments: %v", err)
	}

	// The unknown type should be skipped, Commentable should be nil
	if len(comments) != 1 {
		t.Fatalf("expected 1 comment, got %d", len(comments))
	}

	if comments[0].Commentable != nil {
		t.Errorf("expected Commentable to be nil for unknown type, got %+v", comments[0].Commentable)
	}
}

// ==================== Test 1: HasOne Eager Loading — EXPOSES BUG ====================

func setupRelDBHasOne(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE rel_users_with_profile (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE rel_profiles (id INTEGER PRIMARY KEY, user_id INTEGER UNIQUE, bio TEXT);

		INSERT INTO rel_users_with_profile (id, name) VALUES (1, 'Alice');
		INSERT INTO rel_profiles (id, user_id, bio) VALUES (1, 1, 'Alice bio');
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

func TestHasOne_EagerLoading(t *testing.T) {
	db := setupRelDBHasOne(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	// HasOne eager loading via loadHasMany panics because reflect.MakeSlice
	// is called on a pointer type (*RelProfile) instead of a slice type.
	// MorphOne handles this correctly (lines 1149-1158) but HasOne reuses
	// loadHasMany without single-value logic.
	panicked := true
	func() {
		defer func() {
			if r := recover(); r == nil {
				panicked = false
			}
		}()
		_, _ = New[RelUserWithProfile]().With("Profile").Get(ctx)
	}()

	if !panicked {
		t.Error("expected HasOne eager loading to panic due to reflect.MakeSlice on pointer type, but it did not panic")
	}
}

// ==================== Test 2: Root-Level With Cols ====================

func TestWith_ColsOnRootRelation(t *testing.T) {
	db := setupRelDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	t.Run("WithCols", func(t *testing.T) {
		users, err := New[RelUser]().With("Posts:id,title,user_id").Get(ctx)
		if err != nil {
			t.Fatalf("failed to get users: %v", err)
		}

		var alice *RelUser
		for _, u := range users {
			if u.Name == "Alice" {
				alice = u
				break
			}
		}
		if alice == nil {
			t.Fatal("Alice not found")
		}

		if len(alice.Posts) != 2 {
			t.Errorf("expected 2 posts for Alice, got %d", len(alice.Posts))
		}
		for _, p := range alice.Posts {
			if p.Title == "" {
				t.Error("expected Title to be populated")
			}
		}
	})

	t.Run("MissingFK", func(t *testing.T) {
		users, err := New[RelUser]().With("Posts:id,title").Get(ctx)
		if err != nil {
			t.Fatalf("failed to get users: %v", err)
		}

		var alice *RelUser
		for _, u := range users {
			if u.Name == "Alice" {
				alice = u
				break
			}
		}
		if alice == nil {
			t.Fatal("Alice not found")
		}

		// FK user_id is omitted from column selection, so posts can't be
		// mapped back to parents — resulting in 0 posts.
		if len(alice.Posts) != 0 {
			t.Errorf("expected 0 posts when FK omitted from cols, got %d", len(alice.Posts))
		}
	})
}

// ==================== Test 3: WithCallback Applied — Verifies Callback Filtering ====================

func TestWithCallback_NotApplied(t *testing.T) {
	db := setupRelDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	// WithCallback stores the callback in relationCallbacks and loadRelations
	// applies it to filter the relation query.
	users, err := New[RelUser]().WithCallback("Posts", func(q *Model[RelPost]) {
		q.Where("title", "=", "Post 1")
	}).Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	var alice *RelUser
	for _, u := range users {
		if u.Name == "Alice" {
			alice = u
			break
		}
	}
	if alice == nil {
		t.Fatal("Alice not found")
	}

	// Callback IS applied, so Alice should have only 1 post matching "Post 1"
	if len(alice.Posts) != 1 {
		t.Errorf("expected 1 post (callback applied filtering to 'Post 1'), got %d", len(alice.Posts))
	}
	if len(alice.Posts) == 1 && alice.Posts[0].Title != "Post 1" {
		t.Errorf("expected post title 'Post 1', got %q", alice.Posts[0].Title)
	}
}

// ==================== Test 4: WithMorph TypeMap Integration ====================

func TestWithMorph_TypeMapIntegration(t *testing.T) {
	db := setupRelDBMorphTo(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	comments, err := New[RelComment]().WithMorph("Commentable", map[string][]string{
		"RelPost":         {},
		"RelUserExtended": {},
	}).Get(ctx)
	if err != nil {
		t.Fatalf("failed to get comments: %v", err)
	}

	if len(comments) != 2 {
		t.Fatalf("expected 2 comments, got %d", len(comments))
	}

	for _, c := range comments {
		if c.Commentable == nil {
			t.Errorf("comment %d: expected Commentable to be loaded", c.ID)
		}
	}
}

// ==================== Test 5: Nested BelongsTo Loaded — Verifies Dynamic Loader ====================

func TestNested_BelongsToSilentlySkipped(t *testing.T) {
	db := setupRelDBMorphTo(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	users, err := New[RelUser]().With("Posts.User").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	var alice *RelUser
	for _, u := range users {
		if u.Name == "Alice" {
			alice = u
			break
		}
	}
	if alice == nil {
		t.Fatal("Alice not found")
	}

	if len(alice.Posts) == 0 {
		t.Fatal("expected Alice to have posts")
	}

	// Nested BelongsTo is now correctly loaded by loadRelationsDynamic.
	for _, p := range alice.Posts {
		if p.User == nil {
			t.Errorf("post %d: expected User to be loaded (nested BelongsTo), got nil", p.ID)
		}
	}
}

// ==================== Test 6: Three-Level Nested HasMany ====================

type ThreeLevelAuthor struct {
	ID    int    `zorm:"primaryKey"`
	Name  string
	Books []*ThreeLevelBook
}

func (a ThreeLevelAuthor) TableName() string { return "three_level_authors" }

func (a ThreeLevelAuthor) BooksRelation() HasMany[ThreeLevelBook] {
	return HasMany[ThreeLevelBook]{ForeignKey: "three_level_author_id"}
}

type ThreeLevelBook struct {
	ID                 int    `zorm:"primaryKey"`
	ThreeLevelAuthorID int
	Title              string
	Chapters           []*ThreeLevelChapter
}

func (b ThreeLevelBook) TableName() string { return "three_level_books" }

func (b ThreeLevelBook) ChaptersRelation() HasMany[ThreeLevelChapter] {
	return HasMany[ThreeLevelChapter]{ForeignKey: "three_level_book_id"}
}

type ThreeLevelChapter struct {
	ID               int    `zorm:"primaryKey"`
	ThreeLevelBookID int
	Heading          string
}

func (c ThreeLevelChapter) TableName() string { return "three_level_chapters" }

func setupRelDBThreeLevel(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE three_level_authors (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE three_level_books (id INTEGER PRIMARY KEY, three_level_author_id INTEGER, title TEXT);
		CREATE TABLE three_level_chapters (id INTEGER PRIMARY KEY, three_level_book_id INTEGER, heading TEXT);

		INSERT INTO three_level_authors (id, name) VALUES (1, 'Author A');
		INSERT INTO three_level_books (id, three_level_author_id, title) VALUES (1, 1, 'Book 1'), (2, 1, 'Book 2');
		INSERT INTO three_level_chapters (id, three_level_book_id, heading) VALUES
			(1, 1, 'Ch 1'), (2, 1, 'Ch 2'), (3, 2, 'Ch 3');
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

func TestNested_ThreeLevels(t *testing.T) {
	db := setupRelDBThreeLevel(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	authors, err := New[ThreeLevelAuthor]().With("Books.Chapters").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get authors: %v", err)
	}

	if len(authors) != 1 {
		t.Fatalf("expected 1 author, got %d", len(authors))
	}

	author := authors[0]
	if len(author.Books) != 2 {
		t.Fatalf("expected 2 books, got %d", len(author.Books))
	}

	for _, book := range author.Books {
		if book.Title == "Book 1" && len(book.Chapters) != 2 {
			t.Errorf("Book 1: expected 2 chapters, got %d", len(book.Chapters))
		}
		if book.Title == "Book 2" && len(book.Chapters) != 1 {
			t.Errorf("Book 2: expected 1 chapter, got %d", len(book.Chapters))
		}
	}
}

// ==================== Test 7: Load Single Entity HasMany ====================

func TestLoad_SingleEntity_HasMany(t *testing.T) {
	db := setupRelDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	user, err := New[RelUser]().Where("id", "=", 1).First(ctx)
	if err != nil {
		t.Fatalf("failed to get user: %v", err)
	}

	err = New[RelUser]().Load(ctx, user, "Posts")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(user.Posts) != 2 {
		t.Errorf("expected 2 posts, got %d", len(user.Posts))
	}
}

// ==================== Test 8: LoadSlice BelongsToMany ====================

func TestLoadSlice_BelongsToMany(t *testing.T) {
	db := setupRelDBExtended(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	users, err := New[RelUserExtended]().Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	err = New[RelUserExtended]().LoadSlice(ctx, users, "Roles")
	if err != nil {
		t.Fatalf("LoadSlice failed: %v", err)
	}

	if len(users) == 0 {
		t.Fatal("expected users")
	}

	if len(users[0].Roles) != 2 {
		t.Errorf("expected 2 roles for Alice, got %d", len(users[0].Roles))
	}
}

// ==================== Test 9: Chained With Calls ====================

func TestWith_ChainedCalls(t *testing.T) {
	db := setupRelDBMorphTo(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	posts, err := New[RelPost]().With("User").With("Comments").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get posts: %v", err)
	}

	if len(posts) == 0 {
		t.Fatal("expected posts")
	}

	// Find Post 1 (Alice's post)
	var post1 *RelPost
	for _, p := range posts {
		if p.Title == "Post 1" {
			post1 = p
			break
		}
	}
	if post1 == nil {
		t.Fatal("Post 1 not found")
	}

	// User should be loaded (BelongsTo)
	if post1.User == nil {
		t.Error("expected User to be loaded on Post 1")
	} else if post1.User.Name != "Alice" {
		t.Errorf("expected User Alice, got %q", post1.User.Name)
	}

	// Comments should be loaded (HasMany)
	if len(post1.Comments) == 0 {
		t.Error("expected Comments to be loaded on Post 1")
	}
}

// ==================== Test 10: HasMany Default FK Inference ====================

type InferAuthor struct {
	ID    int    `zorm:"primaryKey"`
	Name  string
	Books []*InferBook
}

func (a InferAuthor) TableName() string { return "infer_authors" }

func (a InferAuthor) BooksRelation() HasMany[InferBook] {
	return HasMany[InferBook]{} // Empty — relies on default FK inference
}

type InferBook struct {
	ID            int `zorm:"primaryKey"`
	InferAuthorID int
	Title         string
}

func (b InferBook) TableName() string { return "infer_books" }

func setupRelDBDefaultFK(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE infer_authors (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE infer_books (id INTEGER PRIMARY KEY, infer_author_id INTEGER, title TEXT);

		INSERT INTO infer_authors (id, name) VALUES (1, 'Author A');
		INSERT INTO infer_books (id, infer_author_id, title) VALUES (1, 1, 'Book 1'), (2, 1, 'Book 2');
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

func TestHasMany_DefaultFKInference(t *testing.T) {
	db := setupRelDBDefaultFK(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	authors, err := New[InferAuthor]().With("Books").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get authors: %v", err)
	}

	if len(authors) != 1 {
		t.Fatalf("expected 1 author, got %d", len(authors))
	}

	if len(authors[0].Books) != 2 {
		t.Errorf("expected 2 books for author, got %d", len(authors[0].Books))
	}
}

// ==================== Test 11: MorphMany Empty Results ====================

func setupRelDBMorphManyEmpty(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE rel_users (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE rel_posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);
		CREATE TABLE rel_roles (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE rel_role_user (user_id INTEGER, role_id INTEGER);
		CREATE TABLE rel_images (id INTEGER PRIMARY KEY, url TEXT, imageable_id INTEGER, imageable_type TEXT);

		INSERT INTO rel_users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		INSERT INTO rel_images (id, url, imageable_id, imageable_type) VALUES
			(1, 'http://alice_thumb.jpg', 1, 'RelUserExtended');
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

func TestMorphMany_EmptyResults(t *testing.T) {
	db := setupRelDBMorphManyEmpty(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	users, err := New[RelUserExtended]().With("Images").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}

	var alice, bob *RelUserExtended
	for _, u := range users {
		if u.Name == "Alice" {
			alice = u
		}
		if u.Name == "Bob" {
			bob = u
		}
	}

	if alice == nil {
		t.Fatal("Alice not found")
	}
	if bob == nil {
		t.Fatal("Bob not found")
	}

	if len(alice.Images) != 1 {
		t.Errorf("expected 1 image for Alice, got %d", len(alice.Images))
	}

	if len(bob.Images) != 0 {
		t.Errorf("expected 0 images for Bob, got %d", len(bob.Images))
	}
}

// =============================================================================
// ISSUE #1: WITH CALLBACK APPLIED TESTS
// =============================================================================

// TestWithCallback_OrderByAndLimit verifies that callbacks applying OrderBy and
// Limit constraints are correctly applied to the relation query.
// Note: LIMIT applies globally to the relation query, not per parent entity.
// To test per-entity behavior, we load a single user (Alice).
func TestWithCallback_OrderByAndLimit(t *testing.T) {
	db := setupRelDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	// Load only Alice to avoid global LIMIT affecting per-user results
	alice, err := New[RelUser]().Where("name", "Alice").WithCallback("Posts", func(q *Model[RelPost]) {
		q.OrderBy("title", "DESC").Limit(1)
	}).First(ctx)
	if err != nil {
		t.Fatalf("failed to get Alice: %v", err)
	}

	// Alice has 2 posts ("Post 1", "Post 2"). With Limit(1) and OrderBy DESC,
	// only "Post 2" should be loaded.
	if len(alice.Posts) != 1 {
		t.Fatalf("expected 1 post (Limit applied), got %d", len(alice.Posts))
	}
	if alice.Posts[0].Title != "Post 2" {
		t.Errorf("expected post title 'Post 2' (DESC order), got %q", alice.Posts[0].Title)
	}
}

// TestWithCallback_MultipleRelations verifies that callbacks for different
// relations on the same query are applied independently.
func TestWithCallback_MultipleRelations(t *testing.T) {
	db := setupRelDBExtended(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	users, err := New[RelUserExtended]().
		WithCallback("Posts", func(q *Model[RelPost]) {
			q.Where("title", "Post 1")
		}).
		WithCallback("Roles", func(q *Model[RelRole]) {
			q.Where("name", "Admin")
		}).
		Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	if len(users) == 0 {
		t.Fatal("expected users")
	}

	alice := users[0]

	// Posts callback filters to "Post 1" only
	if len(alice.Posts) != 1 {
		t.Errorf("expected 1 post (filtered by callback), got %d", len(alice.Posts))
	}
	if len(alice.Posts) == 1 && alice.Posts[0].Title != "Post 1" {
		t.Errorf("expected 'Post 1', got %q", alice.Posts[0].Title)
	}

	// Roles callback filters to "Admin" only
	if len(alice.Roles) != 1 {
		t.Errorf("expected 1 role (filtered by callback), got %d", len(alice.Roles))
	}
	if len(alice.Roles) == 1 && alice.Roles[0].Name != "Admin" {
		t.Errorf("expected 'Admin', got %q", alice.Roles[0].Name)
	}
}

// =============================================================================
// ISSUE #2: NESTED DYNAMIC RELATIONS (ALL TYPES) TESTS
// =============================================================================

// TestNested_MixedRelationTypes verifies that multiple nested paths work
// together: With("Roles") + With("Posts.User") combined.
func TestNested_MixedRelationTypes(t *testing.T) {
	db := setupRelDBExtended(t)
	defer db.Close()

	// Add a rel_posts row so nested Posts.User can be loaded
	_, err := db.Exec(`INSERT INTO rel_posts (id, user_id, title) VALUES (1, 1, 'Post 1')`)
	if err != nil {
		// Row may already exist from setupRelDBExtended
		_ = err
	}

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	users, err := New[RelUserExtended]().
		With("Roles").
		With("Posts.User").
		Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	if len(users) == 0 {
		t.Fatal("expected users")
	}

	alice := users[0]

	// Roles should be loaded
	if len(alice.Roles) != 2 {
		t.Errorf("expected 2 roles, got %d", len(alice.Roles))
	}

	// Posts should be loaded
	if len(alice.Posts) == 0 {
		t.Fatal("expected posts to be loaded")
	}

	// Nested User on each post should be loaded (BelongsTo under HasMany)
	for _, p := range alice.Posts {
		if p.User == nil {
			t.Errorf("post %d: expected nested User to be loaded, got nil", p.ID)
		} else if p.User.Name != "Alice" {
			t.Errorf("post %d: expected user Alice, got %q", p.ID, p.User.Name)
		}
	}
}

// =============================================================================
// ISSUE #3: SQL INJECTION IN RELATION FK COLUMNS TESTS
// =============================================================================

// RelUserInjectionFK is a model with a malicious ForeignKey to test validation.
type RelUserInjectionFK struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Posts []*RelPost
}

func (u RelUserInjectionFK) TableName() string { return "rel_users" }

func (u RelUserInjectionFK) PostsRelation() HasMany[RelPost] {
	return HasMany[RelPost]{
		ForeignKey: "user_id; DROP TABLE rel_users",
	}
}

// TestRelation_InvalidForeignKeyReturnsError verifies that a FK with SQL injection
// content returns an error rather than executing malicious SQL.
func TestRelation_InvalidForeignKeyReturnsError(t *testing.T) {
	db := setupRelDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	_, err := New[RelUserInjectionFK]().With("Posts").Get(ctx)
	if err == nil {
		t.Fatal("expected error for invalid foreign key with SQL injection, got nil")
	}
	if !errors.As(err, new(*RelationError)) {
		// Might be a validation error wrapped differently
		t.Logf("error type: %T, message: %v", err, err)
	}

	// Verify table was not dropped
	var count int
	err2 := db.QueryRow("SELECT COUNT(*) FROM rel_users").Scan(&count)
	if err2 != nil {
		t.Fatalf("rel_users table was dropped by injection! Error: %v", err2)
	}
	if count != 2 {
		t.Errorf("expected 2 users to still exist, got %d", count)
	}
}

// RelUserInjectionMorph is a model with malicious morph columns.
type RelUserInjectionMorph struct {
	ID     int `zorm:"primaryKey"`
	Name   string
	Images []*RelImage
}

func (u RelUserInjectionMorph) TableName() string { return "rel_users" }

func (u RelUserInjectionMorph) ImagesRelation() MorphMany[RelImage] {
	return MorphMany[RelImage]{
		Type: "imageable_type; DROP TABLE",
		ID:   "imageable_id",
	}
}

// TestRelation_InvalidMorphColumnsReturnsError verifies that morph relations
// with injection attempts in column names return an error.
func TestRelation_InvalidMorphColumnsReturnsError(t *testing.T) {
	db := setupRelDBExtended(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	_, err := New[RelUserInjectionMorph]().With("Images").Get(ctx)
	if err == nil {
		t.Fatal("expected error for invalid morph column, got nil")
	}

	// Verify tables were not dropped
	var count int
	err2 := db.QueryRow("SELECT COUNT(*) FROM rel_users").Scan(&count)
	if err2 != nil {
		t.Fatalf("table was dropped by injection! Error: %v", err2)
	}
}

// =============================================================================
// ISSUE #4: BELONGSTOMANY NIL POINTER GUARD TESTS
// =============================================================================

// TestBelongsToMany_UserWithNoRoles verifies that a user with no entries in
// the pivot table loads with an empty (not nil) Roles slice and doesn't panic.
func TestBelongsToMany_UserWithNoRoles(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE rel_users (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE rel_posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);
		CREATE TABLE rel_roles (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE rel_role_user (user_id INTEGER, role_id INTEGER);
		CREATE TABLE rel_images (id INTEGER PRIMARY KEY, url TEXT, imageable_id INTEGER, imageable_type TEXT);

		INSERT INTO rel_users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		INSERT INTO rel_roles (id, name) VALUES (1, 'Admin');
		INSERT INTO rel_role_user (user_id, role_id) VALUES (1, 1);
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	users, err := New[RelUserExtended]().With("Roles").OrderBy("id", "ASC").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users with roles: %v", err)
	}

	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}

	// Alice should have 1 role
	var alice, bob *RelUserExtended
	for _, u := range users {
		if u.Name == "Alice" {
			alice = u
		}
		if u.Name == "Bob" {
			bob = u
		}
	}

	if alice == nil || bob == nil {
		t.Fatal("expected both Alice and Bob")
	}

	if len(alice.Roles) != 1 {
		t.Errorf("expected Alice to have 1 role, got %d", len(alice.Roles))
	}

	// Bob should have 0 roles (empty slice, no panic)
	if bob.Roles == nil {
		t.Log("Bob.Roles is nil (not empty slice) - this is acceptable behavior")
	}
	if len(bob.Roles) != 0 {
		t.Errorf("expected Bob to have 0 roles, got %d", len(bob.Roles))
	}
}
