package zorm

import (
	"context"
	"errors"
	"testing"
)

func TestEntityWrites_RespectScopeAndPrimaryKey(t *testing.T) {
	for _, operation := range []string{"Update", "UpdateColumns", "Save"} {
		for _, mode := range []string{"direct", "cached", "transaction"} {
			for _, allowed := range []bool{false, true} {
				t.Run(operation+"/"+mode+"/"+map[bool]string{false: "blocked", true: "allowed"}[allowed], func(t *testing.T) {
					db := setupSaveDB(t)
					defer db.Close()
					ctx := context.Background()
					base := New[saveItem]().SetDB(db)
					entity, err := base.Find(ctx, 1)
					if err != nil {
						t.Fatal(err)
					}
					defer ClearOriginals(entity)
					entity.Name = "changed"
					m := base.Clone().Where("value", 20)
					if allowed {
						m.OrWhere("value", 10)
					} else {
						m.OrWhere("value", 999)
					}
					if mode == "cached" {
						cache := NewStmtCache(10)
						defer cache.Close()
						m.WithStmtCache(cache)
					}
					write := func(m *Model[saveItem]) error {
						switch operation {
						case "Update":
							return m.Update(ctx, entity)
						case "UpdateColumns":
							return m.UpdateColumns(ctx, entity, "name")
						default:
							return m.Save(ctx, entity)
						}
					}
					if mode == "transaction" {
						err = m.Transaction(ctx, func(tx *Tx) error { return write(m.WithTx(tx)) })
					} else {
						err = write(m)
					}
					if allowed && err != nil {
						t.Fatal(err)
					}
					if !allowed && !errors.Is(err, ErrRecordNotFound) {
						t.Errorf("excluded entity should return ErrRecordNotFound, got %v", err)
					}
					if !allowed && !base.IsDirtyField(entity, "name") {
						t.Error("excluded write marked an unsaved change clean")
					}
					for id, want := range map[int]string{1: "A", 2: "B"} {
						if allowed && id == 1 {
							want = "changed"
						}
						var name string
						if err := db.QueryRow("SELECT name FROM save_items WHERE id = ?", id).Scan(&name); err != nil {
							t.Fatal(err)
						}
						if name != want {
							t.Errorf("id %d: got %q, want %q", id, name, want)
						}
					}
				})
			}
		}
	}
}

func TestSave_ScopeMissIsNotVersionConflict(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	ctx := context.Background()
	m := New[saveVersionedItem]().SetDB(db)
	entity, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(entity)
	entity.Name = "changed"
	if err := m.Where("value", 999).Save(ctx, entity); !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("scope miss should return ErrRecordNotFound, got %v", err)
	}
}

func TestEntityWrites_PreserveCTEAndScopeArgumentOrder(t *testing.T) {
	for _, operation := range []string{"Update", "UpdateColumns", "Save"} {
		t.Run(operation, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			ctx := context.Background()
			entity, err := New[saveItem]().SetDB(db).Find(ctx, 2)
			if err != nil {
				t.Fatal(err)
			}
			defer ClearOriginals(entity)
			entity.Name = "changed"
			scope := New[saveItem]().Where("value", 20)
			m := New[saveItem]().SetDB(db).WithCTE("scope", scope).
				Where("name", "B").Where("id IN (SELECT id FROM scope)")
			switch operation {
			case "Update":
				err = m.Update(ctx, entity)
			case "UpdateColumns":
				err = m.UpdateColumns(ctx, entity, "name")
			case "Save":
				err = m.Save(ctx, entity)
			}
			if err != nil {
				t.Fatal(err)
			}
			fresh, err := New[saveItem]().SetDB(db).Find(ctx, 2)
			if err != nil || fresh.Name != "changed" {
				t.Fatalf("arguments were not bound in SQL order: row=%v, err=%v", fresh, err)
			}
		})
	}
}
