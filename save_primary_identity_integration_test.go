package zorm

import (
	"context"
	"errors"
	"testing"
)

type saveIdentityItem struct {
	ID     int `zorm:"primaryKey"`
	Name   string
	Value  int
	NextID int `zorm:"-"`
}

func (saveIdentityItem) TableName() string { return "save_items" }

func (s *saveIdentityItem) BeforeUpdate(context.Context) error {
	if s.NextID != 0 {
		s.ID = s.NextID
	}
	return nil
}

// A baseline belongs to the row that was loaded, even if the caller or an
// update hook changes the primary key stored on the same entity pointer.
func TestSave_RejectsPrimaryKeyChanges(t *testing.T) {
	for _, mode := range []string{"caller", "hook", "primary only", "cached", "transaction"} {
		t.Run(mode, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			ctx := context.Background()
			entity, err := New[saveIdentityItem]().SetDB(db).Find(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer ClearOriginals(entity)
			if mode == "hook" {
				entity.NextID = 2
			} else {
				entity.ID = 2
			}
			if mode != "primary only" {
				entity.Name = "overwritten"
			}
			m := New[saveIdentityItem]().SetDB(db)
			if mode == "cached" {
				cache := NewStmtCache(10)
				defer cache.Close()
				m.WithStmtCache(cache)
			}
			if mode == "transaction" {
				err = m.Transaction(ctx, func(tx *Tx) error { return m.WithTx(tx).Save(ctx, entity) })
			} else {
				err = m.Save(ctx, entity)
			}
			if !errors.Is(err, ErrInvalidModel) {
				t.Errorf("expected ErrInvalidModel for a changed primary key, got %v", err)
			}
			for id, want := range map[int]string{1: "A", 2: "B"} {
				var name string
				if err := db.QueryRow(`SELECT name FROM save_items WHERE id = ?`, id).Scan(&name); err != nil {
					t.Fatal(err)
				}
				if name != want {
					t.Errorf("row %d name=%q, want %q", id, name, want)
				}
			}
		})
	}
}
