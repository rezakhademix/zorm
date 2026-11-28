package zorm

import (
	"context"
	"testing"
)

type pivotLocalParent struct {
	ID       int
	LegacyID int
	Roles    []*whereHasChild
}

func (pivotLocalParent) TableName() string { return "legacy_parents" }
func (pivotLocalParent) RolesRelation() BelongsToMany[whereHasChild] {
	return BelongsToMany[whereHasChild]{PivotTable: "legacy_pivot", ForeignKey: "user_legacy_id", RelatedKey: "role_id", LocalKey: "legacy_id"}
}

func TestPivotWrites_HonorLocalKey(t *testing.T) {
	for _, operation := range []string{"attach", "detach", "sync"} {
		t.Run(operation, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			if _, err := db.Exec(`CREATE TABLE legacy_pivot (user_legacy_id INTEGER, role_id INTEGER);
			INSERT INTO legacy_pivot VALUES (1, 9), (500, 8)`); err != nil {
				t.Fatal(err)
			}
			m := New[pivotLocalParent]().SetDB(db)
			parent := &pivotLocalParent{ID: 1, LegacyID: 500}
			ctx := context.Background()
			var err error
			switch operation {
			case "attach":
				err = m.Attach(ctx, parent, "Roles", []any{7}, nil)
			case "detach":
				err = m.Detach(ctx, parent, "Roles", nil)
			case "sync":
				err = m.Sync(ctx, parent, "Roles", []any{7}, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			var other, intended int
			if err := db.QueryRow("SELECT COUNT(*) FROM legacy_pivot WHERE user_legacy_id = 1 AND role_id = 9").Scan(&other); err != nil {
				t.Fatal(err)
			}
			if other != 1 {
				t.Error("modified another parent's pivot rows")
			}
			query := "SELECT COUNT(*) FROM legacy_pivot WHERE user_legacy_id = 500"
			want := 0
			if operation == "attach" {
				want = 2
			}
			if operation == "sync" {
				want = 1
				query += " AND role_id = 7"
			}
			if err := db.QueryRow(query).Scan(&intended); err != nil {
				t.Fatal(err)
			}
			if intended != want {
				t.Errorf("intended parent has %d matching rows, want %d", intended, want)
			}
		})
	}
}

func TestBelongsToMany_CallbackOrdersAndLimitsPerParent(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	_, err := db.Exec(`CREATE TABLE legacy_parents (id INTEGER PRIMARY KEY, legacy_id INTEGER);
	CREATE TABLE legacy_pivot (user_legacy_id INTEGER, role_id INTEGER);
	CREATE TABLE wh_children (id INTEGER PRIMARY KEY, code TEXT);
	INSERT INTO legacy_parents VALUES (1, 500), (2, 600);
	INSERT INTO wh_children VALUES (7, 'a'), (8, 'b'), (9, 'c');
	INSERT INTO legacy_pivot VALUES (500, 7), (500, 8), (600, 7), (600, 9)`)
	if err != nil {
		t.Fatal(err)
	}
	parents, err := New[pivotLocalParent]().SetDB(db).WithCallback("Roles", func(q *Model[whereHasChild]) { q.OrderBy("id", "DESC").Limit(1) }).OrderBy("id", "ASC").Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(parents) != 2 {
		t.Fatalf("parents: %d", len(parents))
	}
	for i, parent := range parents {
		defer ClearOriginals(parent)
		for _, role := range parent.Roles {
			defer ClearOriginals(role)
		}
		if len(parent.Roles) != 1 || parent.Roles[0].ID != 8+i {
			t.Errorf("parent %d has wrong roles: %+v", parent.ID, parent.Roles)
		}
	}
}
