package zorm

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// Relation loaders match parents to children by key. BelongsTo normalizes keys
// through anyToKeyString; HasMany/HasOne/Morph historically used the raw value
// as a map key, which breaks when the two sides' Go types differ and panics
// outright on unhashable types such as []byte.

// --- mixed integer widths across the relation ---

type kmParent struct {
	ID   int64 // parent PK is int64
	Name string
	Kids []*kmKid
}

func (kmParent) TableName() string { return "km_parents" }

func (kmParent) KidsRelation() HasMany[kmKid] {
	return HasMany[kmKid]{ForeignKey: "parent_id"}
}

type kmKid struct {
	ID       int64
	ParentID int // child FK is int, not int64
	Label    string
}

func (kmKid) TableName() string { return "km_kids" }

// --- one-to-one with mismatched widths ---

type kmOwner struct {
	ID      int64
	Profile *kmProfile
}

func (kmOwner) TableName() string { return "km_owners" }

func (kmOwner) ProfileRelation() HasOne[kmProfile] {
	return HasOne[kmProfile]{ForeignKey: "owner_id"}
}

type kmProfile struct {
	ID      int64
	OwnerID int
	Bio     string
}

func (kmProfile) TableName() string { return "km_profiles" }

// --- []byte (BLOB) keys ---

type kmBlobParent struct {
	ID   []byte
	Kids []*kmBlobKid
}

func (kmBlobParent) TableName() string { return "km_blob_parents" }

func (kmBlobParent) KidsRelation() HasMany[kmBlobKid] {
	return HasMany[kmBlobKid]{ForeignKey: "parent_id"}
}

type kmBlobKid struct {
	ID       int64
	ParentID []byte
	Label    string
}

func (kmBlobKid) TableName() string { return "km_blob_kids" }

func setupKeyMatchDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	_, err = db.Exec(`
		CREATE TABLE km_parents (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE km_kids (id INTEGER PRIMARY KEY, parent_id INTEGER, label TEXT);
		CREATE TABLE km_owners (id INTEGER PRIMARY KEY);
		CREATE TABLE km_profiles (id INTEGER PRIMARY KEY, owner_id INTEGER, bio TEXT);
		CREATE TABLE km_blob_parents (id BLOB PRIMARY KEY);
		CREATE TABLE km_blob_kids (id INTEGER PRIMARY KEY, parent_id BLOB, label TEXT);

		INSERT INTO km_parents (id, name) VALUES (1, 'p1');
		INSERT INTO km_kids (id, parent_id, label) VALUES (1, 1, 'k1'), (2, 1, 'k2');
		INSERT INTO km_owners (id) VALUES (1);
		INSERT INTO km_profiles (id, owner_id, bio) VALUES (1, 1, 'bio1');
		INSERT INTO km_blob_parents (id) VALUES (X'DEADBEEF');
		INSERT INTO km_blob_kids (id, parent_id, label) VALUES (1, X'DEADBEEF', 'bk1');
	`)
	if err != nil {
		t.Fatalf("failed to setup key-match DB: %v", err)
	}
	return db
}

func TestRelationKeyMatch_HasManyAcrossIntWidths(t *testing.T) {
	db := setupKeyMatchDB(t)
	defer db.Close()

	parents, err := New[kmParent]().SetDB(db).With("Kids").Get(context.Background())
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(parents) != 1 {
		t.Fatalf("expected 1 parent, got %d", len(parents))
	}
	if len(parents[0].Kids) != 2 {
		t.Errorf("expected 2 kids matched across int64/int keys, got %d", len(parents[0].Kids))
	}
}

func TestRelationKeyMatch_HasOneAcrossIntWidths(t *testing.T) {
	db := setupKeyMatchDB(t)
	defer db.Close()

	owners, err := New[kmOwner]().SetDB(db).With("Profile").Get(context.Background())
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(owners) != 1 {
		t.Fatalf("expected 1 owner, got %d", len(owners))
	}
	if owners[0].Profile == nil {
		t.Fatal("expected the profile to be matched across int64/int keys, got nil")
	}
	if owners[0].Profile.Bio != "bio1" {
		t.Errorf("expected bio1, got %q", owners[0].Profile.Bio)
	}
}

func TestRelationKeyMatch_HasManyWithByteSliceKey(t *testing.T) {
	db := setupKeyMatchDB(t)
	defer db.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked on an unhashable relation key: %v", r)
		}
	}()

	parents, err := New[kmBlobParent]().SetDB(db).With("Kids").Get(context.Background())
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(parents) != 1 {
		t.Fatalf("expected 1 parent, got %d", len(parents))
	}
	if len(parents[0].Kids) != 1 {
		t.Errorf("expected 1 kid matched on a []byte key, got %d", len(parents[0].Kids))
	}
}
