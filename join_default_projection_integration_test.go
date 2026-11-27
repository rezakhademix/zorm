package zorm

import (
	"context"
	"testing"
)

func TestJoin_DefaultProjectionKeepsBaseIdentity(t *testing.T) {
	for _, table := range []string{"join_orders", "join_orders AS o", "join_orders o", `"join_orders" AS o`} {
		t.Run(table, func(t *testing.T) {
			db := setupJoinDB(t)
			defer db.Close()
			base := "o"
			if table == "join_orders" {
				base = table
			}
			m := New[JoinOrder]().SetDB(db).Table(table).
				Join("join_users u", base+".user_id", "=", "u.id").Where(base+".id", 2)
			rows, err := m.Get(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 {
				t.Fatalf("got %d orders", len(rows))
			}
			defer ClearOriginals(rows[0])
			if rows[0].ID != 2 {
				t.Fatalf("joined table overwrote order 2's ID: %+v", rows[0])
			}
			rows[0].Amount = 777
			if err := New[JoinOrder]().SetDB(db).Save(context.Background(), rows[0]); err != nil {
				t.Fatal(err)
			}
			var first, second float64
			if err := db.QueryRow("SELECT amount FROM join_orders WHERE id = 1").Scan(&first); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow("SELECT amount FROM join_orders WHERE id = 2").Scan(&second); err != nil {
				t.Fatal(err)
			}
			if first != 100 || second != 777 {
				t.Errorf("Save targeted wrong row: first=%v second=%v", first, second)
			}
		})
	}
}
