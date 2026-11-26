package zorm

import (
	"context"
	"reflect"
	"testing"
)

func TestRelationKeys_DefaultPivotKeysAndExplicitLocalField(t *testing.T) {
	ctx := context.Background()
	for _, dynamic := range []bool{false, true} {
		db := setupFailureRelations(t)
		if _, err := db.Exec(`ALTER TABLE failure_links ADD COLUMN failure_parent_id INTEGER;
			ALTER TABLE failure_links ADD COLUMN failure_child_id INTEGER;
			UPDATE failure_links SET failure_parent_id = 1, failure_child_id = 1`); err != nil {
			t.Fatal(err)
		}
		m := New[failureParent]().SetDB(db)
		parent := failureParentEntity()
		config := &BelongsToMany[failureChild]{PivotTable: "failure_links", LocalKey: "ID", RelatedPK: "id"}
		var err error
		if dynamic {
			err = m.loadBelongsToManyDynamic(ctx, []any{parent}, reflect.TypeFor[failureParent](), config, "Peers", "", nil)
		} else {
			err = m.loadBelongsToMany(ctx, []*failureParent{parent}, config, "Peers", "", nil, nil)
		}
		if err != nil || len(parent.Peers) != 1 || parent.Peers[0].ID != 1 {
			t.Fatalf("default pivot keys failed: dynamic=%v peers=%v err=%v", dynamic, parent.Peers, err)
		}
	}
}

func TestRelationKeys_BelongsToFieldNamesAndSharedOwner(t *testing.T) {
	ctx := context.Background()
	for _, dynamic := range []bool{false, true} {
		m := New[failureParent]().SetDB(setupFailureRelations(t))
		first, second := failureParentEntity(), failureParentEntity()
		second.ID = 2
		config := &BelongsTo[failureChild]{ForeignKey: "OwnerID", OwnerKey: "ID"}
		var err error
		if dynamic {
			err = m.loadBelongsToDynamic(ctx, []any{first, second}, reflect.TypeFor[failureParent](), config, "Owner", "", nil)
		} else {
			err = m.loadBelongsTo(ctx, []*failureParent{first, second}, config, "Owner", "", nil, nil)
		}
		if err != nil || first.Owner == nil || second.Owner == nil || first.Owner != second.Owner || first.Owner.ID != 1 {
			t.Fatalf("shared owner field-name lookup failed: dynamic=%v first=%v second=%v err=%v", dynamic, first.Owner, second.Owner, err)
		}
	}
}

type failureValueParent struct {
	ID      int
	OwnerID int
	Owner   failureChild `zorm:"-"`
}

func (failureValueParent) OwnerRelation() BelongsTo[failureChild] {
	return BelongsTo[failureChild]{ForeignKey: "owner_id"}
}

func TestRelationKeys_ValueRelationField(t *testing.T) {
	ctx := context.Background()
	for _, dynamic := range []bool{false, true} {
		m := New[failureValueParent]().SetDB(setupFailureRelations(t))
		parent := &failureValueParent{ID: 1, OwnerID: 1}
		var err error
		if dynamic {
			err = m.loadRelationsDynamic(ctx, []any{parent}, reflect.TypeFor[failureValueParent](), []string{"Owner"})
		} else {
			err = m.Load(ctx, parent, "Owner")
		}
		if err != nil || parent.Owner.ID != 1 || parent.Owner.Name != "child" {
			t.Fatalf("value relation assignment failed: dynamic=%v owner=%+v err=%v", dynamic, parent.Owner, err)
		}
	}
}

type failurePointerParent struct {
	ID       *int
	Children []*failureChild
	Photos   []*failureChild
}

func (failurePointerParent) MorphType() string { return "failureParent" }
func (failurePointerParent) ChildrenRelation() HasMany[failureChild] {
	return HasMany[failureChild]{ForeignKey: "parent_id"}
}
func (failurePointerParent) PhotosRelation() MorphMany[failureChild] {
	return MorphMany[failureChild]{Type: "owner_type", ID: "owner_id"}
}

func TestRelationKeys_NullableParentPrimaryKey(t *testing.T) {
	ctx := context.Background()
	m := New[failurePointerParent]().SetDB(setupFailureRelations(t))
	for _, parentID := range []*int{nil, snapshotPtr(1)} {
		parent := &failurePointerParent{ID: parentID}
		if err := m.Load(ctx, parent, "Children", "Photos"); err != nil {
			t.Fatal(err)
		}
		want := 0
		if parentID != nil {
			want = 1
		}
		if len(parent.Children) != want || len(parent.Photos) != want {
			t.Errorf("nullable primary key mapped incorrectly: id=%v children=%d photos=%d", parentID, len(parent.Children), len(parent.Photos))
		}
	}
}
