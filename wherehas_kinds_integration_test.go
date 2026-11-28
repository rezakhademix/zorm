package zorm

import (
	"context"
	"errors"
	"testing"
)

type whereHasParent struct {
	ID        int
	Code      string
	OwnerCode string
}

func (whereHasParent) TableName() string { return "wh_parents" }
func (whereHasParent) ChildrenRelation() HasMany[whereHasChild] {
	return HasMany[whereHasChild]{ForeignKey: "parent_code", LocalKey: "code"}
}
func (whereHasParent) ChildRelation() HasOne[whereHasChild] {
	return HasOne[whereHasChild]{ForeignKey: "parent_code", LocalKey: "code"}
}
func (whereHasParent) OwnerRelation() BelongsTo[whereHasChild] {
	return BelongsTo[whereHasChild]{ForeignKey: "owner_code", OwnerKey: "code"}
}
func (whereHasParent) TagsRelation() BelongsToMany[whereHasChild] {
	return BelongsToMany[whereHasChild]{PivotTable: "wh_pivot", ForeignKey: "parent_code", RelatedKey: "child_code", LocalKey: "code", RelatedPK: "code"}
}
func (whereHasParent) ImagesRelation() MorphMany[whereHasChild] {
	return MorphMany[whereHasChild]{Type: "parent_type", ID: "parent_id"}
}
func (whereHasParent) ImageRelation() MorphOne[whereHasChild] {
	return MorphOne[whereHasChild]{Type: "parent_type", ID: "parent_id"}
}
func (whereHasParent) InvalidRelation() string                     { return "invalid" }
func (whereHasParent) NeedsArgRelation(int) HasMany[whereHasChild] { return HasMany[whereHasChild]{} }

type whereHasChild struct {
	ID         int
	Code       string
	ParentCode string
	ParentID   int
	ParentType string
}

func (whereHasChild) TableName() string { return "wh_children" }

func TestWhereHas_RelationKinds(t *testing.T) {
	for _, relation := range []string{"Children", "Child", "Owner", "Tags", "Images", "Image"} {
		t.Run(relation, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			_, err := db.Exec(`CREATE TABLE wh_parents (id INTEGER PRIMARY KEY, code TEXT, owner_code TEXT);
			CREATE TABLE wh_children (id INTEGER PRIMARY KEY, code TEXT, parent_code TEXT, parent_id INTEGER, parent_type TEXT);
			CREATE TABLE wh_pivot (parent_code TEXT, child_code TEXT);
			INSERT INTO wh_parents VALUES (1, 'A', 'X'), (2, 'B', 'Y');
			INSERT INTO wh_children VALUES (9, 'X', 'A', 1, 'whereHasParent'), (10, 'Z', 'C', 2, 'anotherType');
			INSERT INTO wh_pivot VALUES ('A', 'X')`)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := New[whereHasParent]().SetDB(db).WhereHas(relation, func(q *Model[whereHasChild]) { q.Where("code", "X") }).Get(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range rows {
				defer ClearOriginals(row)
			}
			if len(rows) != 1 || rows[0].ID != 1 {
				t.Fatalf("wrong parents: %+v", rows)
			}
		})
	}
}

func TestWhereHas_InvalidRelationFailsClosed(t *testing.T) {
	for _, relation := range []string{"Missing", "Invalid", "NeedsArg"} {
		t.Run(relation, func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Errorf("invalid relation panicked: %v", p)
				}
			}()
			q := New[whereHasParent]().WhereHas(relation, nil)
			if !errors.Is(q.GetBuildErr(), ErrRelationNotFound) && !errors.Is(q.GetBuildErr(), ErrInvalidRelation) {
				t.Fatalf("missing relation accepted: %v", q.GetBuildErr())
			}
		})
	}
}

type whereHasDefaultChild struct {
	ID      int
	OwnerID int
}

func (whereHasDefaultChild) TableName() string { return "wh_default_children" }
func (whereHasDefaultChild) OwnerRelation() BelongsTo[whereHasParent] {
	return BelongsTo[whereHasParent]{}
}

func TestWhereHas_BelongsToDefaultUsesRelationName(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	_, err := db.Exec(`CREATE TABLE wh_parents (id INTEGER PRIMARY KEY);
	CREATE TABLE wh_default_children (id INTEGER PRIMARY KEY, owner_id INTEGER);
	INSERT INTO wh_parents VALUES (10); INSERT INTO wh_default_children VALUES (1, 10), (2, 20)`)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := New[whereHasDefaultChild]().SetDB(db).WhereHas("Owner", nil).Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		defer ClearOriginals(row)
	}
	if len(rows) != 1 || rows[0].ID != 1 {
		t.Fatalf("wrong owner correlation: %+v", rows)
	}
}
