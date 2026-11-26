package zorm

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

type RelMembership struct {
	ID     int
	UserID int
	User   *RelUserExtended
}

func (RelMembership) TableName() string { return "rel_memberships" }

func (RelMembership) UserRelation() BelongsTo[RelUserExtended] {
	return BelongsTo[RelUserExtended]{ForeignKey: "user_id"}
}

func TestRelations_NestedMorphMany(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	_, err = db.Exec(`
		CREATE TABLE rel_users (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE rel_images (id INTEGER PRIMARY KEY, url TEXT, imageable_id INTEGER, imageable_type TEXT);
		CREATE TABLE rel_memberships (id INTEGER PRIMARY KEY, user_id INTEGER);

		INSERT INTO rel_users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');
		INSERT INTO rel_memberships (id, user_id) VALUES (1, 1), (2, 2), (3, 3), (4, 1), (5, 99);
		INSERT INTO rel_images (id, url, imageable_id, imageable_type) VALUES
			(1, 'alice.jpg', 1, 'RelUserExtended'),
			(2, 'post.jpg', 1, 'RelPost'),
			(3, 'alice-thumb.jpg', 1, 'RelUserExtended'),
			(4, 'bob.jpg', 2, 'RelUserExtended');
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}

	ctx := context.Background()
	scope := NewTrackingScope()
	defer scope.Close()

	memberships, err := New[RelMembership]().SetDB(db).WithTrackingScope(scope).
		With("User.Images:id,url,imageable_id").OrderBy("id", "ASC").Get(ctx)
	if err != nil {
		t.Fatalf("failed to load nested images: %v", err)
	}
	if len(memberships) != 5 {
		t.Fatalf("expected 5 memberships, got %d", len(memberships))
	}

	expected := map[int]map[int]string{
		1: {1: "alice.jpg", 3: "alice-thumb.jpg"},
		2: {4: "bob.jpg"},
		3: {},
	}
	for _, membership := range memberships {
		if membership.UserID == 99 {
			if membership.User != nil {
				t.Error("expected missing user to remain nil")
			}
			continue
		}
		if membership.User == nil {
			t.Fatalf("membership %d: expected user to be loaded", membership.ID)
		}
		if membership.User.ID != membership.UserID {
			t.Errorf("membership %d: expected user %d, got %d", membership.ID, membership.UserID, membership.User.ID)
		}
		images := membership.User.Images
		want := expected[membership.UserID]
		if len(images) != len(want) {
			t.Fatalf("user %d: expected %d images, got %d", membership.UserID, len(want), len(images))
		}
		for _, image := range images {
			url, ok := want[image.ID]
			if !ok || image.URL != url || image.ImageableID != membership.UserID {
				t.Errorf("user %d: unexpected image %+v", membership.UserID, image)
			}
			if image.ImageableType != "" {
				t.Error("unselected imageable_type should remain empty")
			}
			if !IsTracked(image) || GetOriginal(image, "url") != url {
				t.Errorf("image %d: expected original URL %q to be tracked", image.ID, url)
			}
		}
	}

	image := memberships[0].User.Images[0]
	image.URL = "updated.jpg"
	if err := New[RelImage]().SetDB(db).Save(ctx, image); err != nil {
		t.Fatalf("failed to save nested image: %v", err)
	}
	var url, imageType string
	if err := db.QueryRow("SELECT url, imageable_type FROM rel_images WHERE id = ?", image.ID).Scan(&url, &imageType); err != nil {
		t.Fatalf("failed to read updated image: %v", err)
	}
	if url != "updated.jpg" || imageType != "RelUserExtended" {
		t.Errorf("expected updated URL and preserved morph type, got %q and %q", url, imageType)
	}

	scope.Close()
	for _, membership := range memberships {
		if IsTracked(membership) {
			t.Error("membership should be untracked after scope closes")
		}
		if membership.User != nil {
			if IsTracked(membership.User) {
				t.Error("user should be untracked after scope closes")
			}
			for _, image := range membership.User.Images {
				if IsTracked(image) {
					t.Error("nested image should be untracked after scope closes")
				}
			}
		}
	}
}
