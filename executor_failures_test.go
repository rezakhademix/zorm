package zorm

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

func TestReadFailures_PropagateDriverErrors(t *testing.T) {
	ctx := context.Background()
	operations := []struct {
		name string
		run  func(*Model[ExModel]) error
	}{
		{"Get", func(m *Model[ExModel]) error { _, err := m.Get(ctx); return err }},
		{"First", func(m *Model[ExModel]) error { _, err := m.First(ctx); return err }},
		{"Find", func(m *Model[ExModel]) error { _, err := m.Find(ctx, 1); return err }},
		{"Pluck", func(m *Model[ExModel]) error { _, err := m.Pluck(ctx, "id"); return err }},
		{"Count", func(m *Model[ExModel]) error { _, err := m.Count(ctx); return err }},
		{"Sum", func(m *Model[ExModel]) error { _, err := m.Sum(ctx, "value"); return err }},
		{"Avg", func(m *Model[ExModel]) error { _, err := m.Avg(ctx, "value"); return err }},
		{"Exists", func(m *Model[ExModel]) error { _, err := m.Exists(ctx); return err }},
		{"FirstOrCreate", func(m *Model[ExModel]) error {
			_, err := m.FirstOrCreate(ctx, map[string]any{"name": "A"}, nil)
			return err
		}},
		{"UpdateOrCreate", func(m *Model[ExModel]) error {
			_, err := m.UpdateOrCreate(ctx, map[string]any{"name": "A"}, nil)
			return err
		}},
	}
	for _, operation := range operations {
		for _, mode := range []string{"query", "prepare", "iteration"} {
			t.Run(operation.name+"/"+mode, func(t *testing.T) {
				failure := &failureDB{columns: []string{"id"}}
				switch mode {
				case "query":
					failure.queryErr = errDatabaseFailure
				case "prepare":
					failure.prepareErr, failure.queryErr = errDatabaseFailure, errDatabaseFailure
				case "iteration":
					failure.iterationErr = errDatabaseFailure
				}
				m := New[ExModel]().SetDB(openFailureDB(t, failure))
				if mode == "prepare" {
					cache := NewStmtCache(10)
					defer cache.Close()
					m = m.WithStmtCache(cache)
				}
				if err := operation.run(m); !errors.Is(err, errDatabaseFailure) {
					t.Fatalf("driver failure lost: %v", err)
				}
			})
		}
	}
}

func TestWriteFailures_PropagateDriverErrors(t *testing.T) {
	ctx := context.Background()
	operations := []struct {
		name string
		run  func(*Model[ExModel], *ExModel) error
	}{
		{"Create", func(m *Model[ExModel], e *ExModel) error { return m.Create(ctx, e) }},
		{"Update", func(m *Model[ExModel], e *ExModel) error { return m.Update(ctx, e) }},
		{"UpdateColumns", func(m *Model[ExModel], e *ExModel) error { return m.UpdateColumns(ctx, e, "name") }},
		{"Save", func(m *Model[ExModel], e *ExModel) error { return m.Save(ctx, e) }},
		{"Delete", func(m *Model[ExModel], _ *ExModel) error { return m.Where("id", 1).Delete(ctx) }},
		{"ForceDeleteAll", func(m *Model[ExModel], _ *ExModel) error { return m.ForceDeleteAll(ctx) }},
		{"UpdateMany", func(m *Model[ExModel], _ *ExModel) error {
			return m.Where("id", 1).UpdateMany(ctx, map[string]any{"name": "changed"})
		}},
		{"DeleteMany", func(m *Model[ExModel], _ *ExModel) error { return m.Where("id", 1).DeleteMany(ctx) }},
		{"CreateMany", func(m *Model[ExModel], e *ExModel) error { return m.CreateMany(ctx, []*ExModel{e}) }},
		{"CreateManyNewPK", func(m *Model[ExModel], e *ExModel) error { return m.CreateManyNewPK(ctx, []*ExModel{e}) }},
		{"UpdateManyByKey", func(m *Model[ExModel], _ *ExModel) error {
			return m.UpdateManyByKey(ctx, "id", "name", map[int]string{1: "changed"})
		}},
		{"BulkInsert", func(m *Model[ExModel], e *ExModel) error { return m.BulkInsert(ctx, []*ExModel{e}) }},
		{"Exec", func(m *Model[ExModel], _ *ExModel) error {
			_, err := m.Raw("UPDATE ex_models SET name = ?", "changed").Exec(ctx)
			return err
		}},
	}
	for _, operation := range operations {
		for _, mode := range []string{"execute", "prepare"} {
			t.Run(operation.name+"/"+mode, func(t *testing.T) {
				failure := &failureDB{queryErr: errDatabaseFailure, execErr: errDatabaseFailure}
				m := New[ExModel]().SetDB(openFailureDB(t, failure))
				if mode == "prepare" {
					failure.prepareErr = errDatabaseFailure
					cache := NewStmtCache(10)
					defer cache.Close()
					m = m.WithStmtCache(cache)
				}
				entity := &ExModel{ID: 1, Name: "A", Value: 10}
				trackOriginals(entity, m.modelInfo)
				defer ClearOriginals(entity)
				entity.Name = "changed"
				if err := operation.run(m, entity); !errors.Is(err, errDatabaseFailure) {
					t.Fatalf("driver failure lost: %v", err)
				}
				if GetOriginal(entity, "name") != "A" {
					t.Error("failed write changed tracking baseline")
				}
			})
		}
	}
}

func TestReadFailures_MalformedRowsAndClosedRows(t *testing.T) {
	ctx := context.Background()
	for _, dynamic := range []bool{false, true} {
		t.Run(map[bool]string{false: "typed", true: "dynamic"}[dynamic], func(t *testing.T) {
			db := openFailureDB(t, &failureDB{columns: []string{"id"}, values: [][]driver.Value{{"not an integer"}}})
			m := New[ExModel]().SetDB(db)
			rows, err := db.QueryContext(ctx, "SELECT id")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			if dynamic {
				_, err = m.scanRowsDynamic(ctx, rows, m.modelInfo)
			} else {
				_, err = m.scanRows(ctx, rows)
			}
			if err == nil || !strings.Contains(err.Error(), "not an integer") {
				t.Fatalf("expected scan conversion failure, got %v", err)
			}
			rows.Close()
			if dynamic {
				_, err = m.scanRowsDynamic(ctx, rows, m.modelInfo)
			} else {
				_, err = m.scanRows(ctx, rows)
			}
			if err == nil {
				t.Error("closed rows should not be readable")
			}
		})
	}
}
