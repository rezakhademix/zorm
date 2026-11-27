package zorm

import (
	"context"
	"reflect"
	"testing"
)

func TestDistinctCount_DefaultProjection(t *testing.T) {
	for _, cached := range []bool{false, true} {
		name := "direct"
		if cached {
			name = "cached"
		}
		t.Run(name, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			if _, err := db.Exec(`UPDATE save_items SET value = 10`); err != nil {
				t.Fatal(err)
			}
			m := New[saveItem]().SetDB(db).Distinct()
			if cached {
				cache := NewStmtCache(10)
				defer cache.Close()
				m.WithStmtCache(cache)
			}
			ctx := context.Background()
			count, err := m.Count(ctx)
			if err != nil || count != 2 {
				t.Errorf("distinct full-row count=%d, err=%v, want 2", count, err)
			}
			count, err = m.Clone().Select("value").Count(ctx)
			if err != nil || count != 1 {
				t.Errorf("distinct value count=%d, err=%v, want 1", count, err)
			}
			count, err = m.Clone().Where("id", 99).Count(ctx)
			if err != nil || count != 0 {
				t.Errorf("empty distinct count=%d, err=%v, want 0", count, err)
			}
			page, err := m.OrderBy("id", "ASC").Paginate(ctx, 2, 1)
			if err != nil {
				t.Fatal(err)
			}
			if page.Total != 2 || page.LastPage != 2 || len(page.Data) != 1 || page.Data[0].ID != 2 {
				t.Fatalf("incorrect distinct pagination: %+v", page)
			}
		})
	}
}

func TestDistinctCount_JoinedDefaultProjection(t *testing.T) {
	ctx := context.Background()
	for _, table := range []string{"join_orders", "join_orders AS o", "join_orders o", `"join_orders" AS o`} {
		for _, mode := range []string{"direct", "cached", "transaction"} {
			t.Run(table+"/"+mode, func(t *testing.T) {
				db := setupJoinDB(t)
				defer db.Close()
				if _, err := db.Exec("INSERT INTO join_payments (id, order_id, status) VALUES (3, 1, 'refunded')"); err != nil {
					t.Fatal(err)
				}
				base := "o"
				if table == "join_orders" {
					base = table
				}
				m := New[JoinOrder]().SetDB(db).Table(table).
					Join("join_payments p", base+".id", "=", "p.order_id").
					Where(base+".user_id", 1).Distinct().OrderBy(base+".id", "ASC")
				if mode == "cached" {
					cache := NewStmtCache(10)
					defer cache.Close()
					m.WithStmtCache(cache)
				}
				run := func(q *Model[JoinOrder]) error {
					before, beforeArgs := q.Print()
					rows, err := q.Get(ctx)
					if err != nil {
						return err
					}
					if len(rows) != 2 || rows[0].ID != 1 || rows[1].ID != 2 {
						t.Fatalf("unexpected distinct orders: %+v", rows)
					}
					count, err := q.Clone().Limit(1).Offset(1).Count(ctx)
					if err != nil || count != int64(len(rows)) {
						t.Errorf("Count=%d, err=%v, want %d distinct orders", count, err, len(rows))
					}
					page, err := q.Paginate(ctx, 1, 2)
					if err != nil {
						return err
					}
					if page.Total != 2 || page.LastPage != 1 || len(page.Data) != 2 || page.Data[0].ID != 1 || page.Data[1].ID != 2 {
						t.Errorf("incorrect distinct pagination: %+v", page)
					}
					page, err = q.Paginate(ctx, 2, 2)
					if err != nil {
						return err
					}
					if page.Total != 2 || page.LastPage != 1 || len(page.Data) != 0 {
						t.Errorf("incorrect empty page: %+v", page)
					}
					count, err = q.Clone().Select(base + ".user_id").Count(ctx)
					if err != nil || count != 1 {
						t.Errorf("explicit user projection Count=%d, err=%v, want 1", count, err)
					}
					count, err = q.Clone().Select(base+".id", "p.id").Count(ctx)
					if err != nil || count != 3 {
						t.Errorf("explicit joined projection Count=%d, err=%v, want 3", count, err)
					}
					count, err = q.Clone().Where(base+".id", 999).Count(ctx)
					if err != nil || count != 0 {
						t.Errorf("empty Count=%d, err=%v, want 0", count, err)
					}
					after, afterArgs := q.Print()
					if before != after || !reflect.DeepEqual(beforeArgs, afterArgs) {
						t.Error("Count or Paginate changed query state")
					}
					return nil
				}
				if mode == "transaction" {
					if err := m.Transaction(ctx, func(tx *Tx) error { return run(m.WithTx(tx)) }); err != nil {
						t.Fatal(err)
					}
				} else if err := run(m); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
