package zorm

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// Scalar counts preserve distinct projections and reject grouped queries.
func TestScalarCount_PreservesDistinctAndRejectsGrouping(t *testing.T) {
	cases := []struct {
		name   string
		apply  func(*ScalarQuery[int])
		want   int64
		reject bool
	}{
		{"distinct", func(q *ScalarQuery[int]) { q.Select("value").Distinct() }, 1, false},
		{"grouped", func(q *ScalarQuery[int]) { q.Select("value").GroupBy("value") }, 0, true},
		{"grouped-default", func(q *ScalarQuery[int]) { q.GroupBy("value") }, 0, true},
		{"having-false", func(q *ScalarQuery[int]) { q.Select("value").GroupBy("value").Having("COUNT(*) > ?", 10) }, 0, true},
		{"having-true", func(q *ScalarQuery[int]) { q.Select("value").GroupBy("value").Having("COUNT(*) > ?", 1) }, 0, true},
		{"ungrouped-having-false", func(q *ScalarQuery[int]) { q.Having("COUNT(*) > ?", 10) }, 0, true},
		{"ungrouped-having-true", func(q *ScalarQuery[int]) { q.Having("COUNT(*) > ?", 0) }, 0, true},
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
					if tc.reject {
						if !errors.Is(err, ErrGroupedAggregateRequiresGet) || n != 0 {
							t.Errorf("expected grouped aggregate rejection, got count=%d, err=%v", n, err)
						}
					} else if err != nil {
						return err
					} else if n != tc.want {
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
