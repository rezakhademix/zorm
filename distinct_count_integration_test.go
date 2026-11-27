package zorm

import (
	"context"
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
