package zorm

import (
	"context"
	"database/sql/driver"
	"testing"
)

// Locking SELECTs must use the primary even when ordinary reads use replicas.
// Distinct returned IDs verify routing through both direct and cached queries.
func TestLock_ReadsUsePrimary(t *testing.T) {
	for _, mode := range []string{"UPDATE", "NO KEY UPDATE", "SHARE", "KEY SHARE"} {
		for _, cached := range []bool{false, true} {
			t.Run(mode+map[bool]string{false: "/direct", true: "/cached"}[cached], func(t *testing.T) {
				primary := openFailureDB(t, &failureDB{
					columns: []string{"id", "name", "value"},
					values:  [][]driver.Value{{int64(1), "primary", int64(10)}},
				})
				replica := openFailureDB(t, &failureDB{
					columns: []string{"id", "name", "value"},
					values:  [][]driver.Value{{int64(2), "replica", int64(20)}},
				})
				oldResolver := GetGlobalResolver()
				ConfigureDBResolver(WithPrimary(primary), WithReplicas(replica))
				t.Cleanup(func() { globalResolver.Store(oldResolver) })

				m := New[saveItem]()
				if cached {
					cache := NewStmtCache(10)
					defer cache.Close()
					m.WithStmtCache(cache)
				}
				for _, forceReplica := range []bool{false, true} {
					q := m.Clone().Lock(mode)
					if forceReplica {
						q.UseReplica(0)
					}
					row, err := q.First(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer ClearOriginals(row)
					if row.ID != 1 {
						t.Errorf("forceReplica=%v: locking read returned replica row %d", forceReplica, row.ID)
					}
				}
				row, err := m.First(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				defer ClearOriginals(row)
				if row.ID != 2 {
					t.Errorf("ordinary read returned primary row %d", row.ID)
				}
			})
		}
	}
}
