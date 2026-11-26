package zorm

import (
	"context"
	"database/sql/driver"
	"reflect"
	"strings"
	"testing"
)

type invalidManyConfig struct{ PivotTable, ForeignKey, RelatedKey, LocalKey, RelatedPK, Table string }
type invalidOwnerConfig struct{ ForeignKey, OwnerKey, Table string }

func TestRelationGuards_RejectNonRelationConfigs(t *testing.T) {
	ctx := context.Background()
	m := New[failureParent]().SetDB(openFailureDB(t, &failureDB{columns: []string{"parent_id", "child_id"}, values: [][]driver.Value{{int64(1), int64(1)}}}))
	parent := failureParentEntity()
	typed, boxed, typ := []*failureParent{parent}, []any{parent}, reflect.TypeFor[failureParent]()
	owner := invalidOwnerConfig{ForeignKey: "owner_id"}
	for _, run := range []func() error{
		func() error { return m.loadHasMany(ctx, typed, struct{}{}, "Children", "", nil, nil) },
		func() error { return m.loadMorphTo(ctx, typed, struct{}{}, "Subject", nil) },
		func() error { return m.loadMorphOneOrMany(ctx, typed, struct{}{}, "Photos", "", nil, nil) },
		func() error { return m.loadBelongsTo(ctx, typed, owner, "Owner", "", nil, nil) },
		func() error { return m.loadHasManyDynamic(ctx, boxed, typ, struct{}{}, "Children", "", nil) },
		func() error { return m.loadBelongsToDynamic(ctx, boxed, typ, owner, "Owner", "", nil) },
		func() error { return m.loadMorphOneOrManyDynamic(ctx, boxed, typ, struct{}{}, "Photos", "", nil) },
	} {
		if err := run(); err == nil {
			t.Error("invalid relation configuration was accepted")
		}
	}
	for _, relatedKey := range []string{"", "child_id"} {
		config := invalidManyConfig{PivotTable: "failure_links", ForeignKey: "parent_id", RelatedKey: relatedKey}
		if err := m.loadBelongsToMany(ctx, typed, config, "Peers", "", nil, nil); err == nil {
			t.Error("non-relation many-to-many accepted")
		}
		if err := m.loadBelongsToManyDynamic(ctx, boxed, typ, config, "Peers", "", nil); err == nil {
			t.Error("non-relation nested many-to-many accepted")
		}
	}
	m.trackRelatedResults(ctx, struct{}{}, []any{&failureChild{ID: 1}})
}

func TestRelationGuards_MissingKeyMetadataAndExplicitLocalColumn(t *testing.T) {
	ctx := context.Background()
	db := setupFailureRelations(t)
	if _, err := db.Exec(`ALTER TABLE failure_children ADD COLUMN unmapped_key INTEGER DEFAULT 1`); err != nil {
		t.Fatal(err)
	}
	m := New[failureParent]().SetDB(db)
	parent := failureParentEntity()
	if err := m.loadHasMany(ctx, []*failureParent{parent}, HasMany[failureChild]{ForeignKey: "unmapped_key"}, "Children", "", nil, nil); err == nil || !strings.Contains(err.Error(), "foreign key column") {
		t.Errorf("unmapped foreign key not rejected: %v", err)
	}
	for _, dynamic := range []bool{false, true} {
		config := parent.PeersRelation()
		config.RelatedPK = "unmapped_key"
		var err error
		if dynamic {
			err = m.loadBelongsToManyDynamic(ctx, []any{parent}, reflect.TypeFor[failureParent](), config, "Peers", "", nil)
		} else {
			err = m.loadBelongsToMany(ctx, []*failureParent{parent}, config, "Peers", "", nil, nil)
		}
		if err == nil || !strings.Contains(err.Error(), "join key column") {
			t.Errorf("unmapped join key not rejected: %v", err)
		}
		config = parent.PeersRelation()
		config.LocalKey = "id"
		if dynamic {
			err = m.loadBelongsToManyDynamic(ctx, []any{parent}, reflect.TypeFor[failureParent](), config, "Peers", "", nil)
		} else {
			err = m.loadBelongsToMany(ctx, []*failureParent{parent}, config, "Peers", "", nil, nil)
		}
		if err != nil || len(parent.Peers) != 1 {
			t.Fatalf("explicit local column failed: %v", err)
		}
		parent.Peers = nil
		config.LocalKey = "Missing"
		if dynamic {
			err = m.loadBelongsToManyDynamic(ctx, []any{parent}, reflect.TypeFor[failureParent](), config, "Peers", "", nil)
		} else {
			err = m.loadBelongsToMany(ctx, []*failureParent{parent}, config, "Peers", "", nil, nil)
		}
		if err != nil || len(parent.Peers) != 0 {
			t.Errorf("missing local key should not match unrelated rows: %v", err)
		}
	}
}

func TestRelationGuards_ForeignFieldFallbacksAndMissingMorphFields(t *testing.T) {
	ctx := context.Background()
	m := New[failureParent]().SetDB(setupFailureRelations(t))
	for _, dynamic := range []bool{false, true} {
		for _, foreign := range []string{"Owner_id", "_owner_id", "missing_id"} {
			parent := failureParentEntity()
			config := BelongsTo[failureChild]{ForeignKey: foreign}
			var err error
			if dynamic {
				err = m.loadBelongsToDynamic(ctx, []any{parent}, reflect.TypeFor[failureParent](), config, "Owner", "", nil)
			} else {
				err = m.loadBelongsTo(ctx, []*failureParent{parent}, config, "Owner", "", nil, nil)
			}
			if err != nil || (parent.Owner == nil) != (foreign == "missing_id") {
				t.Errorf("foreign field fallback failed: %q %v %v", foreign, parent.Owner, err)
			}
		}
	}
	for _, fields := range [][2]string{{"missing", "subject_id"}, {"kind", "missing"}} {
		parent := failureParentEntity()
		config := parent.SubjectRelation()
		config.Type, config.ID = fields[0], fields[1]
		if err := m.loadMorphTo(ctx, []*failureParent{parent}, config, "Subject", nil); err != nil || parent.Subject != nil {
			t.Errorf("missing morph field was not skipped: %v", err)
		}
	}
	if rc := extractRelationConstraints(HasMany[failureChild]{}, nil, ctx, m.db); rc != nil {
		t.Error("nil callback generated constraints")
	}
	if rc := extractRelationConstraints(MorphTo[any]{}, func(any) {}, ctx, m.db); rc != nil {
		t.Error("dynamic morph target should not construct an untyped query")
	}
	if rc := extractRelationConstraints(HasMany[failureChild]{}, 42, ctx, m.db); rc != nil {
		t.Error("non-function callback generated constraints")
	}
}
