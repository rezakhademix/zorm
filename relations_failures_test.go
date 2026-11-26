package zorm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type failureParent struct {
	ID        int `zorm:"primaryKey"`
	OwnerID   *int
	Kind      *string
	SubjectID *int
	Children  []*failureChild
	Child     *failureChild
	Owner     *failureChild
	Peers     []*failureChild
	Photos    []*failureChild
	Photo     *failureChild
	Subject   any `zorm:"-"`
}

func (failureParent) TableName() string { return "failure_parents" }
func (failureParent) ChildrenRelation() HasMany[failureChild] {
	return HasMany[failureChild]{ForeignKey: "parent_id"}
}
func (failureParent) ChildRelation() HasOne[failureChild] {
	return HasOne[failureChild]{ForeignKey: "parent_id"}
}
func (failureParent) OwnerRelation() BelongsTo[failureChild] {
	return BelongsTo[failureChild]{ForeignKey: "owner_id"}
}
func (failureParent) PeersRelation() BelongsToMany[failureChild] {
	return BelongsToMany[failureChild]{PivotTable: "failure_links", ForeignKey: "parent_id", RelatedKey: "child_id"}
}
func (failureParent) PhotosRelation() MorphMany[failureChild] {
	return MorphMany[failureChild]{Type: "owner_type", ID: "owner_id"}
}
func (failureParent) PhotoRelation() MorphOne[failureChild] {
	return MorphOne[failureChild]{Type: "owner_type", ID: "owner_id"}
}
func (failureParent) SubjectRelation() MorphTo[any] {
	return MorphTo[any]{Type: "kind", ID: "subject_id", TypeMap: map[string]any{"child": failureChild{}}}
}

type failureChild struct {
	ID        int `zorm:"primaryKey"`
	ParentID  int
	OwnerID   int
	OwnerType string
	Name      string
}

func (failureChild) TableName() string { return "failure_children" }
func (c *failureChild) AfterFind(context.Context) error {
	if c.Name == "reject" {
		return errDatabaseFailure
	}
	return nil
}

func failureParentEntity() *failureParent {
	return &failureParent{ID: 1, OwnerID: snapshotPtr(1), Kind: snapshotPtr("child"), SubjectID: snapshotPtr(1)}
}

func setupFailureRelations(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	_, err = db.Exec(`
		CREATE TABLE failure_parents (id INTEGER PRIMARY KEY, owner_id INTEGER, kind TEXT, subject_id INTEGER);
		CREATE TABLE failure_children (id INTEGER PRIMARY KEY, parent_id INTEGER, owner_id INTEGER, owner_type TEXT, name TEXT);
		CREATE TABLE failure_links (parent_id INTEGER, child_id INTEGER);
		INSERT INTO failure_parents VALUES (1, 1, 'child', 1);
		INSERT INTO failure_children VALUES (1, 1, 1, 'failureParent', 'child');
		INSERT INTO failure_links VALUES (1, 1);
	`)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestRelationFailures_LoadersPropagateErrors(t *testing.T) {
	ctx := context.Background()
	for _, relation := range []string{"Children", "Child", "Owner", "Peers", "Photos", "Photo", "Subject"} {
		for _, dynamic := range []bool{false, true} {
			if dynamic && relation == "Subject" {
				continue // MorphTo has its own typed dispatcher.
			}
			for _, mode := range []string{"query", "iteration", "scan", "hook", "nested", "related query"} {
				if mode == "related query" && relation != "Peers" {
					continue
				}
				path := "typed"
				if dynamic {
					path = "dynamic"
				}
				t.Run(relation+"/"+path+"/"+mode, func(t *testing.T) {
					failure := &failureDB{query: func(query string) (driver.Rows, error) {
						pivot := strings.Contains(query, "FROM failure_links")
						if mode == "query" || mode == "related query" && !pivot {
							return nil, errDatabaseFailure
						}
						if mode == "iteration" {
							return &failureRows{columns: []string{"id"}, err: errDatabaseFailure}, nil
						}
						if pivot {
							if mode == "scan" {
								return &failureRows{columns: []string{"parent_id"}, values: [][]driver.Value{{int64(1)}}}, nil
							}
							return &failureRows{columns: []string{"parent_id", "child_id"}, values: [][]driver.Value{{int64(1), int64(1)}}}, nil
						}
						if mode == "scan" {
							return &failureRows{columns: []string{"id"}, values: [][]driver.Value{{"bad id"}}}, nil
						}
						name := "child"
						if mode == "hook" {
							name = "reject"
						}
						return &failureRows{columns: []string{"id", "parent_id", "owner_id", "name"}, values: [][]driver.Value{{int64(1), int64(1), int64(1), name}}}, nil
					}}
					m := New[failureParent]().SetDB(openFailureDB(t, failure))
					entity := failureParentEntity()
					relName := relation
					if mode == "nested" {
						if relation == "Subject" {
							m = m.WithMorph("Subject", map[string][]string{"child": {"Missing"}})
						} else {
							relName += ".Missing"
						}
					}
					var err error
					if dynamic {
						err = m.loadRelationsDynamic(ctx, []any{entity}, reflect.TypeFor[failureParent](), []string{relName})
					} else {
						err = m.Load(ctx, entity, relName)
					}
					if err == nil {
						t.Fatal("relation load silently accepted a failure")
					}
					switch mode {
					case "scan":
						if !strings.Contains(err.Error(), "Scan") && !strings.Contains(err.Error(), "scan") {
							t.Errorf("expected scan failure, got %v", err)
						}
					case "nested":
						if !strings.Contains(err.Error(), "Missing") {
							t.Errorf("nested error lost context: %v", err)
						}
					default:
						if !errors.Is(err, errDatabaseFailure) {
							t.Errorf("original error lost: %v", err)
						}
					}
				})
			}
		}
	}
}

func TestRelationFailures_EmptyAndMissingTargets(t *testing.T) {
	ctx := context.Background()
	for _, dynamic := range []bool{false, true} {
		for _, ownerID := range []*int{nil, snapshotPtr(0), snapshotPtr(99)} {
			m := New[failureParent]().SetDB(setupFailureRelations(t))
			entity := &failureParent{ID: 1, OwnerID: ownerID}
			var err error
			if dynamic {
				err = m.loadRelationsDynamic(ctx, []any{entity}, reflect.TypeFor[failureParent](), []string{"Owner"})
			} else {
				err = m.Load(ctx, entity, "Owner")
			}
			if err != nil || entity.Owner != nil {
				t.Fatalf("missing target should remain nil: owner=%v err=%v", entity.Owner, err)
			}
		}
	}
	for _, entity := range []*failureParent{
		{ID: 1},
		{ID: 1, Kind: snapshotPtr("")},
		{ID: 1, Kind: snapshotPtr("child")},
		{ID: 1, Kind: snapshotPtr("unknown"), SubjectID: snapshotPtr(1)},
	} {
		m := New[failureParent]().SetDB(setupFailureRelations(t))
		if err := m.Load(ctx, entity, "Subject"); err != nil || entity.Subject != nil {
			t.Fatalf("empty or unregistered morph should be skipped: subject=%v err=%v", entity.Subject, err)
		}
	}
}
