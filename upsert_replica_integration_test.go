package zorm

import (
	"context"
	"testing"
)

// Find-or-create decisions must use the primary, including duplicate recovery.
func TestUpserts_LookUpOnPrimary(t *testing.T) {
	for _, operation := range []string{"FirstOrCreate", "UpdateOrCreate"} {
		for _, replicaState := range []string{"missing", "stale"} {
			for _, cached := range []bool{false, true} {
				name := operation + "/" + replicaState + "/direct"
				if cached {
					name = operation + "/" + replicaState + "/cached"
				}
				t.Run(name, func(t *testing.T) {
					primary := setupFocDB(t)
					replica := setupFocDB(t)
					if _, err := primary.Exec(`INSERT INTO foc_users VALUES (1, 'a@example.com', 'existing')`); err != nil {
						t.Fatal(err)
					}
					if replicaState == "stale" {
						if _, err := replica.Exec(`INSERT INTO foc_users VALUES (99, 'a@example.com', 'stale')`); err != nil {
							t.Fatal(err)
						}
					}
					old := GetGlobalResolver()
					defer globalResolver.Store(old)
					ConfigureDBResolver(WithPrimary(primary), WithReplicas(replica))
					m := New[FocUser]()
					if cached {
						cache := NewStmtCache(10)
						defer cache.Close()
						m.WithStmtCache(cache)
					}
					ctx := context.Background()
					attributes := map[string]any{"email": "a@example.com"}
					values := map[string]any{"name": "updated"}
					var entity *FocUser
					var err error
					want := "existing"
					if operation == "FirstOrCreate" {
						entity, err = m.FirstOrCreate(ctx, attributes, values)
					} else {
						entity, err = m.UpdateOrCreate(ctx, attributes, values)
						want = "updated"
					}
					if err != nil {
						t.Fatal(err)
					}
					defer ClearOriginals(entity)
					if entity.ID != 1 || entity.Name != want {
						t.Fatalf("returned replica state: %+v, want primary ID 1/name %q", entity, want)
					}
					var stored string
					if err := primary.QueryRow(`SELECT name FROM foc_users WHERE id = 1`).Scan(&stored); err != nil || stored != want {
						t.Fatalf("primary name=%q, err=%v, want %q", stored, err, want)
					}
					if m.forcePrimary {
						t.Fatal("lookup changed the caller's routing preference")
					}
				})
			}
		}
	}
}
