package zorm

import (
	"context"
	"testing"
)

type WhereShadowBase struct {
	Name string `zorm:"column:base_name"`
}

type whereShadowRecord struct {
	ID int
	WhereShadowBase
	Name string
}

func (whereShadowRecord) TableName() string { return "where_shadow_records" }

// The zero check and bound value must come from the same indexed field.
func TestWhereStruct_ShadowedFieldsUseTheirOwnValues(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	_, err := db.Exec(`CREATE TABLE where_shadow_records (id INTEGER PRIMARY KEY, base_name TEXT, name TEXT);
		INSERT INTO where_shadow_records VALUES (1, 'A', 'X'), (2, 'B', 'Y')`)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		filter whereShadowRecord
		id     int
	}{
		{"embedded only", whereShadowRecord{WhereShadowBase: WhereShadowBase{Name: "A"}}, 1},
		{"outer only", whereShadowRecord{Name: "Y"}, 2},
		{"both", whereShadowRecord{WhereShadowBase: WhereShadowBase{Name: "A"}, Name: "X"}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := New[whereShadowRecord]().SetDB(db).Where(&tc.filter).Get(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || rows[0].ID != tc.id {
				t.Fatalf("shadowed predicate selected %d rows, want only ID %d", len(rows), tc.id)
			}
		})
	}
}
