package zorm

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestRelations_NestedBelongsToMany_EmptyPivot(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	// An empty pivot must not query the related table, which is absent here.
	_, err = db.Exec(`
		CREATE TABLE rel_users (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE rel_memberships (id INTEGER PRIMARY KEY, user_id INTEGER);
		CREATE TABLE rel_role_user (user_id INTEGER, role_id INTEGER);
		INSERT INTO rel_users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		INSERT INTO rel_memberships (id, user_id) VALUES (1, 1), (2, 2);
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}

	scope := NewTrackingScope()
	defer scope.Close()
	memberships, err := New[RelMembership]().SetDB(db).WithTrackingScope(scope).
		With("User.Roles").Get(context.Background())
	if err != nil {
		t.Fatalf("failed to load nested roles with an empty pivot: %v", err)
	}
	if len(memberships) != 2 {
		t.Fatalf("expected 2 memberships, got %d", len(memberships))
	}
	for _, membership := range memberships {
		if membership.User == nil || membership.User.ID != membership.UserID {
			t.Fatalf("membership %d: expected user %d to be loaded", membership.ID, membership.UserID)
		}
		if len(membership.User.Roles) != 0 {
			t.Errorf("user %d: expected no roles, got %d", membership.UserID, len(membership.User.Roles))
		}
	}
}

func TestRelations_NestedBelongsToMany_LoadAndSave(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	_, err = db.Exec(`
		CREATE TABLE rel_users (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE rel_memberships (id INTEGER PRIMARY KEY, user_id INTEGER);
		CREATE TABLE rel_roles (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE rel_role_user (user_id INTEGER, role_id INTEGER);
		INSERT INTO rel_users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');
		INSERT INTO rel_memberships (id, user_id) VALUES (1, 1), (2, 2), (3, 3);
		INSERT INTO rel_roles (id, name) VALUES (10, 'Admin'), (11, 'Editor');
		INSERT INTO rel_role_user (user_id, role_id) VALUES (1, 10), (1, 11), (2, 11), (2, 99);
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}

	ctx := context.Background()
	scope := NewTrackingScope()
	defer scope.Close()
	memberships, err := New[RelMembership]().SetDB(db).WithTrackingScope(scope).
		With("User.Roles:id,name").OrderBy("id", "ASC").Get(ctx)
	if err != nil {
		t.Fatalf("failed to load nested roles: %v", err)
	}
	if len(memberships) != 3 {
		t.Fatalf("expected 3 memberships, got %d", len(memberships))
	}

	expected := map[int]map[int]string{
		1: {10: "Admin", 11: "Editor"},
		2: {11: "Editor"},
		3: {},
	}
	for _, membership := range memberships {
		if membership.User == nil || membership.User.ID != membership.UserID {
			t.Fatalf("membership %d: expected user %d to be loaded", membership.ID, membership.UserID)
		}
		got := make(map[int]string)
		for _, role := range membership.User.Roles {
			got[role.ID] = role.Name
			if !IsTracked(role) || GetOriginal(role, "name") != role.Name {
				t.Errorf("role %d: expected original name to be tracked", role.ID)
			}
		}
		want := expected[membership.UserID]
		if len(membership.User.Roles) != len(want) || !reflect.DeepEqual(got, want) {
			t.Errorf("user %d: expected roles %v, got %v", membership.UserID, want, got)
		}
	}

	roles := memberships[0].User.Roles
	if len(roles) != 2 {
		t.Fatalf("expected 2 roles for Alice, got %d", len(roles))
	}
	role := roles[0]
	role.Name = "Updated"
	if err := New[RelRole]().SetDB(db).Save(ctx, role); err != nil {
		t.Fatalf("failed to save nested role: %v", err)
	}
	var name string
	if err := db.QueryRow("SELECT name FROM rel_roles WHERE id = ?", role.ID).Scan(&name); err != nil {
		t.Fatalf("failed to read updated role: %v", err)
	}
	if name != "Updated" {
		t.Errorf("expected updated role name, got %q", name)
	}
}
