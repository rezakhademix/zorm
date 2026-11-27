package zorm

import (
	"context"
	"reflect"
	"testing"
)

type existsAliasRecord struct {
	Label string
	Total int
}

func (existsAliasRecord) TableName() string { return "save_items" }

func TestExists_PreservesGroupedProjectionAliases(t *testing.T) {
	for _, mode := range []string{"direct", "cached", "transaction"} {
		t.Run(mode, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			ctx := context.Background()
			m := New[existsAliasRecord]().SetDB(db).
				WithCTE("eligible", New[saveItem]().SetDB(db).Where("value", ">", 0)).Table("eligible").
				Select("name AS label", "COUNT(*) AS total").GroupBy("label").Having("COUNT(*) > ?", 0)
			if mode == "cached" {
				cache := NewStmtCache(10)
				defer cache.Close()
				m.WithStmtCache(cache)
			}
			run := func(q *Model[existsAliasRecord]) error {
				rows, err := q.Get(ctx)
				if err != nil {
					return err
				}
				if len(rows) != 2 {
					t.Fatalf("Get returned %d groups", len(rows))
				}
				for _, row := range rows {
					defer ClearOriginals(row)
				}
				before, beforeArgs := q.Print()
				exists, err := q.Exists(ctx)
				if err != nil {
					return err
				}
				if !exists {
					t.Error("grouped projection lost its matches")
				}
				after, afterArgs := q.Print()
				if before != after || !reflect.DeepEqual(beforeArgs, afterArgs) {
					t.Error("Exists mutated the query")
				}
				exists, err = q.Clone().Having("COUNT(*) > ?", 10).Exists(ctx)
				if exists {
					t.Error("Exists ignored HAVING")
				}
				return err
			}
			var err error
			if mode == "transaction" {
				err = m.Transaction(ctx, func(tx *Tx) error { return run(m.WithTx(tx)) })
			} else {
				err = run(m)
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
