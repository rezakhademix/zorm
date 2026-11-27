package zorm

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
)

func TestRelationReload_ClearsMissingMatches(t *testing.T) {
	t.Run("HasMany", func(t *testing.T) {
		testRelationReload(t, setupRelDB, &RelUser{ID: 1}, "Posts", "DELETE FROM rel_posts", true)
	})
	t.Run("BelongsTo", func(t *testing.T) {
		testRelationReload(t, setupRelDB, &RelPost{ID: 1, UserID: 1}, "User", "DELETE FROM rel_users", true)
	})
	t.Run("HasOne", func(t *testing.T) {
		setup := func(t *testing.T) *sql.DB {
			db := setupRelDB(t)
			if _, err := db.Exec(`CREATE TABLE rel_profiles (id INTEGER PRIMARY KEY, user_id INTEGER, bio TEXT);
				INSERT INTO rel_profiles VALUES (1, 1, 'profile')`); err != nil {
				t.Fatal(err)
			}
			return db
		}
		testRelationReload(t, setup, &RelUserWithProfile{ID: 1}, "Profile", "DELETE FROM rel_profiles", true)
	})
	t.Run("BelongsToMany", func(t *testing.T) {
		testRelationReload(t, setupRelDBExtended, &RelUserExtended{ID: 1}, "Roles", "DELETE FROM rel_role_user", true)
	})
	t.Run("MorphMany", func(t *testing.T) {
		testRelationReload(t, setupRelDBExtended, &RelUserExtended{ID: 1}, "Images", "DELETE FROM rel_images", true)
	})
	t.Run("MorphOne", func(t *testing.T) {
		testRelationReload(t, setupRelDBMorphOne, &RelUserWithAvatar{ID: 1}, "Avatar", "DELETE FROM rel_images", true)
	})
	t.Run("MorphTo", func(t *testing.T) {
		parent := &RelComment{ID: 1, CommentableID: 1, CommentableType: "RelUserExtended"}
		testRelationReload(t, setupRelDBMorphTo, parent, "Commentable", "DELETE FROM rel_users", false)
	})
}

// Reloading replaces the relation, including when its last match was deleted.
func testRelationReload[T any](t *testing.T, setup func(*testing.T) *sql.DB, parent *T, relation, deleteSQL string, nested bool) {
	t.Helper()
	paths := []string{"typed"}
	if nested {
		paths = append(paths, "dynamic")
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			db := setup(t)
			defer db.Close()
			ctx := context.Background()
			m := New[T]().SetDB(db)
			load := func() error {
				if path == "dynamic" {
					return m.loadRelationsDynamic(ctx, []any{parent}, reflect.TypeFor[T](), []string{relation})
				}
				return m.Load(ctx, parent, relation)
			}
			if err := load(); err != nil {
				t.Fatal(err)
			}
			field := m.modelInfo.GetRelationField(reflect.ValueOf(parent).Elem(), relation)
			if !field.IsValid() || field.IsZero() {
				t.Fatal("expected the initial relation to have a match")
			}
			if _, err := db.Exec(deleteSQL); err != nil {
				t.Fatal(err)
			}
			if err := load(); err != nil {
				t.Fatal(err)
			}
			if field.Kind() == reflect.Slice {
				if field.Len() != 0 {
					t.Fatalf("reload retained %d deleted children", field.Len())
				}
			} else if !field.IsZero() {
				t.Fatal("reload retained a deleted related record")
			}
		})
	}
}

func TestRelationReload_ClearsMissingForeignKey(t *testing.T) {
	db := setupRelDB(t)
	defer db.Close()
	ctx := context.Background()
	m := New[RelPost]().SetDB(db)
	post := &RelPost{ID: 1, UserID: 1}
	if err := m.Load(ctx, post, "User"); err != nil {
		t.Fatal(err)
	}
	post.UserID = 0
	if err := m.Load(ctx, post, "User"); err != nil {
		t.Fatal(err)
	}
	if post.User != nil {
		t.Fatal("reload retained the previous user after its foreign key was cleared")
	}
}
