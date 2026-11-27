package zorm

import (
	"context"
	"fmt"
	"testing"
)

// Unsupported predicates must fail instead of silently widening a query.
func TestWhere_RejectsUnsupportedConditions(t *testing.T) {
	for _, input := range []any{123, true, []int{1}, nil, (*saveItem)(nil)} {
		for _, operator := range []string{"Where", "OrWhere"} {
			t.Run(operator+"/"+fmt.Sprintf("%T", input), func(t *testing.T) {
				db := setupSaveDB(t)
				defer db.Close()
				m := New[saveItem]().SetDB(db).Where("value", ">", 0)
				if operator == "Where" {
					m.Where(input)
				} else {
					m.OrWhere(input)
				}
				if err := m.UpdateMany(context.Background(), map[string]any{"name": "changed"}); err == nil {
					t.Error("unsupported predicate did not prevent the write")
				}
				var n int
				if err := db.QueryRow(`SELECT COUNT(*) FROM save_items WHERE name = 'changed'`).Scan(&n); err != nil {
					t.Fatal(err)
				}
				if n != 0 {
					t.Fatalf("unsupported predicate allowed %d rows to change", n)
				}
			})
		}
	}
}
