package zorm

import (
	"context"
	"errors"
	"testing"
)

func TestColumnBuilders_RejectExpressions(t *testing.T) {
	for _, column := range []string{"pg_sleep(10)", "(select id)", "(case (select ascii(substr(password,1,1)) from admins limit 1) when 97 then id else name end)"} {
		for name, build := range map[string]func(*Model[TestModel]){
			"OrderBy":    func(m *Model[TestModel]) { m.OrderBy(column, "ASC") },
			"GroupBy":    func(m *Model[TestModel]) { m.GroupBy(column) },
			"DistinctBy": func(m *Model[TestModel]) { m.DistinctBy(column) },
			"Join":       func(m *Model[TestModel]) { m.Join("others", column, "=", "others.id") },
			"WhereNull":  func(m *Model[TestModel]) { m.WhereNull(column) },
		} {
			t.Run(name+"/"+column, func(t *testing.T) {
				m := New[TestModel]()
				build(m)
				if !errors.Is(m.GetBuildErr(), ErrInvalidColumnName) {
					t.Fatalf("expression accepted: %v", m.GetBuildErr())
				}
			})
		}
	}
}

func TestColumnTerminals_RejectExpressions(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	ctx := context.Background()
	for name, run := range map[string]func(*Model[saveItem]) error{
		"Pluck":     func(m *Model[saveItem]) error { _, err := m.Pluck(ctx, "abs(value)"); return err },
		"Sum":       func(m *Model[saveItem]) error { _, err := m.Sum(ctx, "abs(value)"); return err },
		"Avg":       func(m *Model[saveItem]) error { _, err := m.Avg(ctx, "abs(value)"); return err },
		"Min":       func(m *Model[saveItem]) error { _, err := m.Min(ctx, "abs(value)"); return err },
		"Max":       func(m *Model[saveItem]) error { _, err := m.Max(ctx, "abs(value)"); return err },
		"CountOver": func(m *Model[saveItem]) error { _, err := m.CountOver(ctx, "abs(value)"); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := run(New[saveItem]().SetDB(db)); !errors.Is(err, ErrInvalidColumnName) {
				t.Fatalf("expression accepted: %v", err)
			}
		})
	}
}
