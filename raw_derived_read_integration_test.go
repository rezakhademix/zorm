package zorm

import (
	"context"
	"database/sql/driver"
	"errors"
	"reflect"
	"testing"
)

func TestRaw_DerivedReadsRejectBeforeSQL(t *testing.T) {
	ctx := context.Background()
	operations := []struct {
		name string
		run  func(*Model[saveItem]) error
	}{
		{"Count", func(m *Model[saveItem]) error { _, err := m.Count(ctx); return err }},
		{"Sum", func(m *Model[saveItem]) error { _, err := m.Sum(ctx, "value"); return err }},
		{"Avg", func(m *Model[saveItem]) error { _, err := m.Avg(ctx, "value"); return err }},
		{"Min", func(m *Model[saveItem]) error { _, err := m.Min(ctx, "value"); return err }},
		{"Max", func(m *Model[saveItem]) error { _, err := m.Max(ctx, "value"); return err }},
		{"Exists", func(m *Model[saveItem]) error { _, err := m.Exists(ctx); return err }},
		{"CountOver", func(m *Model[saveItem]) error { _, err := m.CountOver(ctx, "name"); return err }},
		{"Pluck", func(m *Model[saveItem]) error { _, err := m.Pluck(ctx, "id"); return err }},
		{"Find", func(m *Model[saveItem]) error { _, err := m.Find(ctx, 2); return err }},
		{"Paginate", func(m *Model[saveItem]) error { _, err := m.Paginate(ctx, 1, 10); return err }},
		{"SimplePaginate", func(m *Model[saveItem]) error { _, err := m.SimplePaginate(ctx, 1, 10); return err }},
		{"Chunk", func(m *Model[saveItem]) error {
			return m.Chunk(ctx, 10, func([]*saveItem) error { t.Error("rejected Raw query invoked callback"); return nil })
		}},
		{"FirstOrCreate", func(m *Model[saveItem]) error {
			_, err := m.FirstOrCreate(ctx, map[string]any{"name": "new"}, nil)
			return err
		}},
		{"UpdateOrCreate", func(m *Model[saveItem]) error {
			_, err := m.UpdateOrCreate(ctx, map[string]any{"name": "new"}, nil)
			return err
		}},
	}
	for _, op := range operations {
		for _, mode := range []string{"direct", "cached", "transaction"} {
			t.Run(op.name+"/"+mode, func(t *testing.T) {
				queries := 0
				f := &failureDB{query: func(string) (driver.Rows, error) { queries++; return nil, errDatabaseFailure }}
				db := openFailureDB(t, f)
				m := New[saveItem]().SetDB(db).Raw("SELECT * FROM save_items WHERE id = ?", 1)
				cache := NewStmtCache(10)
				defer cache.Close()
				if mode == "cached" {
					m.WithStmtCache(cache)
				}
				run := func(q *Model[saveItem]) error {
					before, beforeArgs := q.Print()
					if err := op.run(q); !errors.Is(err, ErrInvalidModel) {
						t.Errorf("expected unsupported Raw rejection, got %v", err)
					}
					after, afterArgs := q.Print()
					if before != after || !reflect.DeepEqual(beforeArgs, afterArgs) {
						t.Error("rejected read changed raw query")
					}
					return nil
				}
				if mode == "transaction" {
					if err := m.Transaction(ctx, func(tx *Tx) error { return run(m.WithTx(tx)) }); err != nil {
						t.Fatal(err)
					}
				} else {
					_ = run(m)
				}
				if queries != 0 || cache.Len() != 0 {
					t.Errorf("rejected read reached SQL: queries=%d cache=%d", queries, cache.Len())
				}
			})
		}
	}
}

func TestRaw_MappedReadsRemainSupported(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	ctx := context.Background()
	m := New[saveItem]().SetDB(db).Raw("SELECT * FROM save_items WHERE id = ?", 2)
	rows, err := m.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != 2 {
		t.Fatalf("raw Get returned wrong row: %v", rows)
	}
	defer ClearOriginals(rows[0])
	first, err := m.First(ctx)
	if err != nil || first.ID != 2 {
		t.Fatalf("raw First=%v, err=%v", first, err)
	}
	defer ClearOriginals(first)
	cursor, err := m.Cursor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if !cursor.Next() {
		t.Fatalf("raw Cursor empty: %v", cursor.Err())
	}
	item, err := cursor.Scan(ctx)
	if err != nil || item.ID != 2 {
		t.Fatalf("raw Cursor=%v, err=%v", item, err)
	}
	defer ClearOriginals(item)
}
