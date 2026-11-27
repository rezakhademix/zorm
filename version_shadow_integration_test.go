package zorm

import (
	"context"
	"errors"
	"testing"
)

type ShadowVersionBase struct {
	Version int64 `zorm:"version"`
}
type ordinaryVersionOverride struct {
	ID int
	ShadowVersionBase
	Version int64
	Name    string
}

func (ordinaryVersionOverride) TableName() string { return "save_versioned_items" }

type taggedVersionOverride struct {
	ID int
	ShadowVersionBase
	Version int64 `zorm:"version"`
	Name    string
}

func (taggedVersionOverride) TableName() string { return "save_versioned_items" }

func TestVersionShadow_UntaggedOuterFieldIsOrdinary(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	ctx := context.Background()
	m := New[ordinaryVersionOverride]().SetDB(db)
	if m.modelInfo.VersionField != nil {
		t.Error("hidden embedded version still enables optimistic locking")
	}
	item, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(item)
	item.Name = "changed"
	if err := m.Save(ctx, item); err != nil {
		t.Fatal(err)
	}
	if item.Version != 1 || item.ShadowVersionBase.Version != 0 {
		t.Errorf("ordinary version was managed: %+v", item)
	}
	item.Version = 7
	if err := m.Save(ctx, item); err != nil {
		t.Fatal(err)
	}
	var name string
	var version int64
	if err := db.QueryRow("SELECT name, version FROM save_versioned_items WHERE id = 1").Scan(&name, &version); err != nil {
		t.Fatal(err)
	}
	if name != "changed" || version != 7 {
		t.Errorf("outer values not persisted: name=%q version=%d", name, version)
	}
}

func TestVersionShadow_TaggedOuterFieldOwnsLocking(t *testing.T) {
	defer func() {
		if p := recover(); p != nil {
			t.Errorf("one mapped version column rejected as duplicate: %v", p)
		}
	}()
	db := setupSaveDB(t)
	defer db.Close()
	ctx := context.Background()
	m := New[taggedVersionOverride]().SetDB(db)
	if m.modelInfo.VersionField != m.modelInfo.Columns["version"] {
		t.Fatal("version metadata does not use the surviving column")
	}
	item, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(item)
	item.Name = "changed"
	if err := m.Save(ctx, item); err != nil {
		t.Fatal(err)
	}
	if item.Version != 2 || item.ShadowVersionBase.Version != 0 {
		t.Errorf("wrong version field managed: %+v", item)
	}
	if _, err := db.Exec("UPDATE save_versioned_items SET version = 3 WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	item.Name = "stale"
	if err := m.Save(ctx, item); !errors.Is(err, ErrOptimisticLock) {
		t.Errorf("outer version lost conflict detection: %v", err)
	}
}
