package zorm

import (
	"context"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestRelationBounds_RejectOversizedBindLists(t *testing.T) {
	original := GetDialect()
	SetDialect(DialectSQLite)
	defer SetDialect(original)
	parents := make([]*failureParent, maxInArgs+1)
	boxed := make([]any, len(parents))
	ids := make([]any, len(parents))
	kind := "child"
	for i := range parents {
		id := i + 1
		parents[i] = &failureParent{ID: id, OwnerID: &id, Kind: &kind, SubjectID: &id}
		boxed[i], ids[i] = parents[i], id
	}
	queries := 0
	db := openFailureDB(t, &failureDB{query: func(string) (driver.Rows, error) {
		queries++
		return nil, errDatabaseFailure
	}})
	m := New[failureParent]().SetDB(db)
	for _, relation := range []string{"Children", "Owner", "Peers", "Photos", "Subject"} {
		for _, dynamic := range []bool{false, true} {
			if dynamic && relation == "Subject" {
				continue
			}
			var err error
			if dynamic {
				err = m.loadRelationsDynamic(context.Background(), boxed, reflect.TypeFor[failureParent](), []string{relation})
			} else {
				err = m.LoadSlice(context.Background(), parents, relation)
			}
			if err == nil || !strings.Contains(err.Error(), "parameter limit") {
				t.Errorf("oversized %s load was not rejected: dynamic=%v err=%v", relation, dynamic, err)
			}
		}
	}
	if queries != 0 {
		t.Errorf("oversized relation loads reached database %d times", queries)
	}
	if err := m.Detach(context.Background(), parents[0], "Peers", ids); err == nil || !strings.Contains(err.Error(), "parameter limit") {
		t.Errorf("oversized pivot detach was not rejected: %v", err)
	}
}

func (failureParent) EmptyMorphRelation() MorphMany[failureChild] {
	return MorphMany[failureChild]{}
}
func (failureParent) InvalidMorphTypeRelation() MorphMany[failureChild] {
	return MorphMany[failureChild]{Type: "owner_type;drop", ID: "owner_id"}
}
func (failureParent) InvalidMorphIDRelation() MorphMany[failureChild] {
	return MorphMany[failureChild]{Type: "owner_type", ID: "owner_id;drop"}
}

func TestRelationBounds_RejectInvalidMorphConfiguration(t *testing.T) {
	ctx := context.Background()
	m := New[failureParent]().SetDB(openFailureDB(t, &failureDB{queryErr: errDatabaseFailure}))
	for _, relation := range []string{"EmptyMorph", "InvalidMorphType", "InvalidMorphID", "EmptyPivot", "Missing"} {
		if err := m.Load(ctx, failureParentEntity(), relation); err == nil || errors.Is(err, errDatabaseFailure) {
			t.Errorf("invalid relation configuration must fail before querying: %s err=%v", relation, err)
		}
	}
	for _, relation := range []string{"EmptyMorph", "EmptyPivot"} {
		if err := m.loadRelationsDynamic(ctx, []any{failureParentEntity()}, reflect.TypeFor[failureParent](), []string{relation}); err == nil || errors.Is(err, errDatabaseFailure) {
			t.Errorf("invalid nested relation configuration must fail before querying: %s err=%v", relation, err)
		}
	}
}
