package zorm

import (
	"context"
	"testing"
)

func TestSave_ShadowedNamesTrackEveryMappedColumn(t *testing.T) {
	for _, mode := range []string{"direct", "cached", "transaction"} {
		t.Run(mode, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			if _, err := db.Exec(`CREATE TABLE where_shadow_records (id INTEGER PRIMARY KEY, base_name TEXT, name TEXT);
				INSERT INTO where_shadow_records VALUES (1, 'base', 'outer')`); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			m := New[whereShadowRecord]().SetDB(db)
			if mode == "cached" {
				cache := NewStmtCache(10)
				defer cache.Close()
				m.WithStmtCache(cache)
			}
			run := func(q *Model[whereShadowRecord]) error {
				item, err := q.Find(ctx, 1)
				if err != nil {
					return err
				}
				defer ClearOriginals(item)
				if GetOriginal(item, "base_name") != "base" {
					t.Error("embedded column missing from baseline")
				}
				item.WhereShadowBase.Name = "changed"
				if !q.HasDirtyFields(item) || q.GetDirtyFields(item)["base_name"] != "changed" {
					t.Error("embedded edit missing from dirty fields")
				}
				if err := q.Save(ctx, item); err != nil {
					return err
				}
				var base, outer string
				if err := q.queryer().QueryRowContext(ctx, "SELECT base_name, name FROM where_shadow_records WHERE id = 1").Scan(&base, &outer); err != nil {
					return err
				}
				if base != "changed" || outer != "outer" {
					t.Errorf("Save lost mapped values: base=%q outer=%q", base, outer)
				}
				if q.HasDirtyFields(item) {
					t.Error("persisted columns remain dirty")
				}
				return nil
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
