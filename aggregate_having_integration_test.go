package zorm

import (
	"context"
	"fmt"
	"testing"
)

func TestAggregateTerminals_PreserveHaving(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		for _, cached := range []bool{false, true} {
			for _, minimum := range []int{0, 10} {
				t.Run(fmt.Sprintf("grouped=%t/cached=%t/minimum=%d", grouped, cached, minimum), func(t *testing.T) {
					db := setupSaveDB(t)
					defer db.Close()
					ctx := context.Background()
					m := New[saveItem]().SetDB(db).
						WithCTE("eligible", New[saveItem]().SetDB(db).Where("id", ">", 0)).
						Table("eligible").Where("value", ">", 0).Having("COUNT(*) > ?", minimum)
					if grouped {
						m.GroupBy("value")
					}
					if cached {
						cache := NewStmtCache(10)
						defer cache.Close()
						m.WithStmtCache(cache)
					}
					wantCount := int64(2)
					wantExists := true
					if minimum == 10 {
						wantCount = 0
						wantExists = false
					}
					count, err := m.Count(ctx)
					if err != nil || count != wantCount {
						t.Errorf("Count=%d, err=%v, want %d", count, err, wantCount)
					}
					exists, err := m.Exists(ctx)
					if err != nil || exists != wantExists {
						t.Errorf("Exists=%t, err=%v, want %t", exists, err, wantExists)
					}
				})
			}
		}
	}
}
