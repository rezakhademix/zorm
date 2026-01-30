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
