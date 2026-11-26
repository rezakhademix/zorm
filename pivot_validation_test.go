package zorm

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
)

func (failureParent) EmptyPivotRelation() BelongsToMany[failureChild] {
	return BelongsToMany[failureChild]{}
}
func (failureParent) InvalidPivotRelation() BelongsToMany[failureChild] {
	return BelongsToMany[failureChild]{PivotTable: "failure_links;drop", ForeignKey: "parent_id", RelatedKey: "child_id"}
}
func (failureParent) InvalidForeignRelation() BelongsToMany[failureChild] {
	return BelongsToMany[failureChild]{PivotTable: "failure_links", ForeignKey: "parent_id;drop", RelatedKey: "child_id"}
}
func (failureParent) InvalidRelatedRelation() BelongsToMany[failureChild] {
	return BelongsToMany[failureChild]{PivotTable: "failure_links", ForeignKey: "parent_id", RelatedKey: "child_id;drop"}
}
func (failureParent) MissingRelatedRelation() BelongsToMany[failureChild] {
	return BelongsToMany[failureChild]{PivotTable: "failure_links", ForeignKey: "parent_id"}
}
func (failureParent) PointerPivotRelation() *BelongsToMany[failureChild] {
	relation := (failureParent{}).PeersRelation()
	return &relation
}
func (failureParent) DefaultForeignRelation() BelongsToMany[failureChild] {
	return BelongsToMany[failureChild]{PivotTable: "failure_links", RelatedKey: "child_id"}
}

func TestPivotValidation_RejectsInvalidConfigurations(t *testing.T) {
	ctx := context.Background()
	for _, operation := range []string{"Attach", "Detach", "Sync"} {
		for _, relation := range []string{"Missing", "Children", "EmptyPivot", "InvalidPivot", "InvalidForeign", "InvalidRelated", "MissingRelated"} {
			t.Run(operation+"/"+relation, func(t *testing.T) {
				m := New[failureParent]().SetDB(openFailureDB(t, &failureDB{queryErr: errDatabaseFailure, execErr: errDatabaseFailure}))
				var err error
				switch operation {
				case "Attach":
					err = m.Attach(ctx, failureParentEntity(), relation, []any{1}, nil)
				case "Detach":
					err = m.Detach(ctx, failureParentEntity(), relation, []any{1})
				case "Sync":
					err = m.Sync(ctx, failureParentEntity(), relation, []any{1}, nil)
				}
				if err == nil || errors.Is(err, errDatabaseFailure) {
					t.Fatalf("configuration must fail before database access: %v", err)
				}
			})
		}
	}
	m := New[failureParent]().SetDB(setupFailureRelations(t))
	if err := m.Attach(ctx, failureParentEntity(), "Peers", []any{1}, map[any]map[string]any{1: {"invalid;column": 1}}); !errors.Is(err, ErrInvalidColumnName) {
		t.Errorf("invalid pivot data identifier accepted: %v", err)
	}
}

func TestPivotValidation_PointerConfigurationAndDefaultForeignKey(t *testing.T) {
	ctx := context.Background()
	for _, relation := range []string{"PointerPivot", "DefaultForeign"} {
		t.Run(relation, func(t *testing.T) {
			db := setupFailureRelations(t)
			if _, err := db.Exec(`DELETE FROM failure_links; ALTER TABLE failure_links ADD COLUMN failure_parent_id INTEGER`); err != nil {
				t.Fatal(err)
			}
			m := New[failureParent]().SetDB(db)
			entity := failureParentEntity()
			if err := m.Attach(ctx, entity, relation, []any{1}, nil); err != nil {
				t.Fatal(err)
			}
			if err := m.Sync(ctx, entity, relation, []any{1, 2}, nil); err != nil {
				t.Fatal(err)
			}
			var count int
			if err := db.QueryRow(`SELECT COUNT(*) FROM failure_links`).Scan(&count); err != nil || count != 2 {
				t.Fatalf("sync did not preserve/add IDs: count=%d err=%v", count, err)
			}
			if err := m.Detach(ctx, entity, relation, []any{1}); err != nil {
				t.Fatal(err)
			}
			if err := m.Detach(ctx, entity, relation, nil); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT COUNT(*) FROM failure_links`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("detach did not remove IDs: count=%d err=%v", count, err)
			}
		})
	}
}

func TestPivotFailures_SyncDoesNotIgnoreReadErrors(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"query", "scan", "iteration", "detach", "attach"} {
		t.Run(mode, func(t *testing.T) {
			failure := &failureDB{columns: []string{"child_id"}}
			writes := 0
			failure.onExec = func(string) { writes++ }
			switch mode {
			case "query":
				failure.queryErr = errDatabaseFailure
			case "scan":
				failure.columns, failure.values = []string{"a", "b"}, [][]driver.Value{{int64(1), int64(2)}}
			case "iteration":
				failure.values, failure.iterationErr = [][]driver.Value{{int64(1)}}, errDatabaseFailure
			case "detach":
				failure.values, failure.execErr = [][]driver.Value{{int64(2)}}, errDatabaseFailure
			case "attach":
				failure.execErr = errDatabaseFailure
			}
			m := New[failureParent]().SetDB(openFailureDB(t, failure))
			err := m.Sync(ctx, failureParentEntity(), "Peers", []any{3}, nil)
			if err == nil || mode != "scan" && !errors.Is(err, errDatabaseFailure) {
				t.Fatalf("pivot failure was lost: %v", err)
			}
			if (mode == "query" || mode == "scan" || mode == "iteration") && writes != 0 {
				t.Errorf("pivot read failure issued %d writes", writes)
			}
		})
	}
}
