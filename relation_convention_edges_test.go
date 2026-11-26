package zorm

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
)

type defaultKeyParent struct {
	ID       int
	Children []*defaultKeyChild
}
type defaultKeyChild struct {
	ID                 int
	DefaultKeyParentID int
}

func (defaultKeyParent) TableName() string { return "default_parents" }
func (defaultKeyChild) TableName() string  { return "default_children" }
func (defaultKeyParent) ChildrenRelation() *HasMany[defaultKeyChild] {
	return &HasMany[defaultKeyChild]{}
}

// DirectChildFields permits a bare relation method without conflicting with its field.
type DirectChildFields struct{ Children []*defaultKeyChild }
type directChildParent struct {
	DirectChildFields
	ID int
}

func (directChildParent) Children() HasMany[defaultKeyChild] {
	return HasMany[defaultKeyChild]{ForeignKey: "default_key_parent_id"}
}

type whereHasMethodParent struct{ ID int }

func (whereHasMethodParent) TableName() string { return "default_parents" }
func (whereHasMethodParent) Value() HasMany[defaultKeyChild] {
	return HasMany[defaultKeyChild]{ForeignKey: "default_key_parent_id"}
}
func (*whereHasMethodParent) Pointer() HasMany[defaultKeyChild] {
	return HasMany[defaultKeyChild]{ForeignKey: "default_key_parent_id"}
}
func (whereHasMethodParent) SuffixedValueRelation() HasMany[defaultKeyChild] {
	return HasMany[defaultKeyChild]{ForeignKey: "default_key_parent_id"}
}
func (*whereHasMethodParent) SuffixedPointerRelation() HasMany[defaultKeyChild] {
	return HasMany[defaultKeyChild]{ForeignKey: "default_key_parent_id"}
}

func setupConventionDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(`CREATE TABLE default_parents (id INTEGER PRIMARY KEY);
		CREATE TABLE default_children (id INTEGER PRIMARY KEY, default_key_parent_id INTEGER);
		INSERT INTO default_parents VALUES (1), (2); INSERT INTO default_children VALUES (1, 1)`); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestRelationConventions_DefaultKeysPointerConfigAndBareMethods(t *testing.T) {
	ctx := context.Background()
	db := setupConventionDB(t)
	m := New[defaultKeyParent]().SetDB(db)
	for _, dynamic := range []bool{false, true} {
		parent := &defaultKeyParent{ID: 1}
		var err error
		if dynamic {
			err = m.loadRelationsDynamic(ctx, []any{parent}, reflect.TypeFor[defaultKeyParent](), []string{"Children"})
		} else {
			err = m.Load(ctx, parent, "Children")
		}
		if err != nil || len(parent.Children) != 1 || parent.Children[0].ID != 1 {
			t.Fatalf("default key/pointer config failed: dynamic=%v err=%v", dynamic, err)
		}
	}
	bare := &directChildParent{ID: 1}
	if err := New[directChildParent]().SetDB(db).Load(ctx, bare, "Children"); err != nil || len(bare.DirectChildFields.Children) != 1 {
		t.Fatalf("bare relation method failed: %v", err)
	}
	rows, err := m.WhereHas("Children", New[defaultKeyChild]().Where("id", 1)).Get(ctx)
	if err != nil || len(rows) != 1 || rows[0].ID != 1 {
		t.Fatalf("default WHERE EXISTS with builder constraints failed: %v", err)
	}
	for _, name := range []string{"Value", "Pointer", "SuffixedValue", "SuffixedPointer"} {
		rows, err := New[whereHasMethodParent]().SetDB(db).WhereHas(name, nil).Get(ctx)
		if err != nil || len(rows) != 1 || rows[0].ID != 1 {
			t.Errorf("%s method lookup failed: %v %v", name, rows, err)
		}
	}
}

func TestRelationConventions_GlobalCallbackAndNestedColumns(t *testing.T) {
	ctx := context.Background()
	db := setupConventionDB(t)
	oldAtomic, oldLegacy, oldResolver := globalDB.Load(), GlobalDB, GetGlobalResolver()
	defer func() { globalDB.Store(oldAtomic); GlobalDB = oldLegacy; globalResolver.Store(oldResolver) }()
	SetGlobalDB(db)
	ClearDBResolver()
	parent := &defaultKeyParent{ID: 1}
	m := New[defaultKeyParent]().SetDB(nil).WithCallback("Children", func(q *Model[defaultKeyChild]) { q.Where("id", 1) })
	if err := m.Load(ctx, parent); err != nil || len(parent.Children) != 1 {
		t.Fatalf("global callback DB fallback failed: %v", err)
	}
	nested := New[failureParent]().SetDB(setupFailureRelations(t))
	err := nested.loadRelationsDynamic(ctx, []any{failureParentEntity()}, reflect.TypeFor[failureParent](), []string{"Photos.Missing:id"})
	if err == nil || !strings.Contains(err.Error(), "Missing") {
		t.Errorf("nested column suffix was lost: %v", err)
	}
	if err := nested.loadRelationsDynamic(ctx, nil, reflect.TypeFor[failureParent](), []string{"Photos"}); err != nil {
		t.Errorf("empty nested result set failed: %v", err)
	}
	for _, dynamic := range []bool{false, true} {
		entity := failureParentEntity()
		config := BelongsTo[failureChild]{}
		if dynamic {
			err = nested.loadBelongsToDynamic(ctx, []any{entity}, reflect.TypeFor[failureParent](), config, "Owner", "", nil)
		} else {
			err = nested.loadBelongsTo(ctx, []*failureParent{entity}, config, "Owner", "", nil, nil)
		}
		if err != nil || entity.Owner == nil {
			t.Errorf("default belongs-to key failed: %v", err)
		}
	}
}
