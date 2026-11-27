package zorm

import (
	"context"
	"reflect"
	"testing"
)

// Scalar counts must describe the qualifying rows or groups, without pagination.
func TestScalarCount_PreservesDistinctAndGrouping(t *testing.T) {
	cases := []struct {
		name  string
		apply func(*ScalarQuery[int])
		want  int64
	}{
		{"distinct", func(q *ScalarQuery[int]) { q.Select("value").Distinct() }, 1},
		{"grouped", func(q *ScalarQuery[int]) { q.Select("value").GroupBy("value") }, 1},
		{"grouped-default", func(q *ScalarQuery[int]) { q.GroupBy("value") }, 1},
		{"having-false", func(q *ScalarQuery[int]) { q.Select("value").GroupBy("value").Having("COUNT(*) > ?", 10) }, 0},
		{"having-true", func(q *ScalarQuery[int]) { q.Select("value").GroupBy("value").Having("COUNT(*) > ?", 1) }, 1},
		{"ungrouped-having-false", func(q *ScalarQuery[int]) { q.Having("COUNT(*) > ?", 10) }, 0},
		{"ungrouped-having-true", func(q *ScalarQuery[int]) { q.Having("COUNT(*) > ?", 0) }, 2},
	}
	for _, tc := range cases {
		for _, transactional := range []bool{false, true} {
			name := tc.name + "/direct"
			if transactional {
				name = tc.name + "/transaction"
			}
			t.Run(name, func(t *testing.T) {
				db := setupSaveDB(t)
				defer db.Close()
				if _, err := db.Exec(`UPDATE save_items SET value = 10`); err != nil {
					t.Fatal(err)
				}
				ctx := context.Background()
				q := Query[int]().SetDB(db).Table("save_items").Where("id", ">", 0).OrderBy("value", "ASC").Limit(1).Offset(1)
				tc.apply(q)
				count := func(tx *Tx) error {
					if tx != nil {
						q = q.Clone().WithTx(tx)
					}
					before, beforeArgs := q.Print()
					n, err := q.Count(ctx)
					if err != nil {
						return err
					}
					if n != tc.want {
						t.Errorf("count=%d, want %d", n, tc.want)
					}
					after, afterArgs := q.Print()
					if before != after || !reflect.DeepEqual(beforeArgs, afterArgs) {
						t.Error("Count mutated the caller's query")
					}
					return nil
				}
				var err error
				if transactional {
					err = New[saveItem]().SetDB(db).Transaction(ctx, count)
				} else {
					err = count(nil)
				}
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
