package zorm

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestFind_JoinedPrimaryKeyIsQualified(t *testing.T) {
	for _, table := range []string{"join_orders", "join_orders AS o", "join_orders o"} {
		for _, mode := range []string{"direct", "cached", "transaction"} {
			t.Run(table+"/"+mode, func(t *testing.T) {
				db := setupJoinDB(t)
				defer db.Close()
				ctx := context.Background()
				base := "o"
				if table == "join_orders" {
					base = table
				}
				m := New[JoinOrder]().SetDB(db).Table(table).
					Join("join_users u", base+".user_id", "=", "u.id").Select(base+".*").
					Where(base+".amount", ">", 150)
				if mode == "cached" {
					cache := NewStmtCache(10)
					defer cache.Close()
					m.WithStmtCache(cache)
				}
				run := func(q *Model[JoinOrder]) error {
					before, beforeArgs := q.Print()
					item, err := q.Find(ctx, 2)
					if err != nil {
						return err
					}
					defer ClearOriginals(item)
					if item.ID != 2 || item.Amount != 200 {
						t.Errorf("Find returned wrong order: %+v", item)
					}
					if _, err := q.Find(ctx, 1); !errors.Is(err, ErrRecordNotFound) {
						t.Errorf("Find lost caller scope: %v", err)
					}
					after, afterArgs := q.Print()
					if before != after || !reflect.DeepEqual(beforeArgs, afterArgs) {
						t.Error("Find changed caller query")
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
}
