package zorm

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestExecutor_StmtCacheWrites(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE ex_models (id INTEGER PRIMARY KEY, value INTEGER, name TEXT)"); err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	cache := NewStmtCache(10)
	defer cache.Close()
	ctx := context.Background()
	model := New[ExModel]().SetDB(db).WithStmtCache(cache)

	entity := &ExModel{Name: "Created", Value: 10}
	if err := model.Create(ctx, entity); err != nil {
		t.Fatalf("cached Create failed: %v", err)
	}
	if entity.ID == 0 {
		t.Fatal("expected Create to populate the primary key")
	}
	entity.Name, entity.Value = "Updated", 20
	if err := model.Update(ctx, entity); err != nil {
		t.Fatalf("cached Update failed: %v", err)
	}
	entity.Name, entity.Value = "Selected", 999
	if err := model.UpdateColumns(ctx, entity, "name"); err != nil {
		t.Fatalf("cached UpdateColumns failed: %v", err)
	}
	var name string
	var value int
	if err := db.QueryRow("SELECT name, value FROM ex_models WHERE id = ?", entity.ID).Scan(&name, &value); err != nil {
		t.Fatalf("failed to read updated row: %v", err)
	}
	if name != "Selected" || value != 20 {
		t.Errorf("expected selected name and preserved value, got %q and %d", name, value)
	}

	if err := model.Clone().Where("id", entity.ID).DeleteMany(ctx); err != nil {
		t.Fatalf("cached DeleteMany failed: %v", err)
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM ex_models").Scan(&count); err != nil || count != 0 {
		t.Fatalf("expected no rows after DeleteMany, got %d, err %v", count, err)
	}
	batch := []*ExModel{{Name: "First", Value: 30}, {Name: "Second", Value: 40}}
	if err := model.CreateManyNewPK(ctx, batch); err != nil {
		t.Fatalf("cached CreateManyNewPK failed: %v", err)
	}
	if batch[0].ID == 0 || batch[1].ID == 0 || batch[0].ID == batch[1].ID {
		t.Fatalf("expected distinct generated primary keys, got %d and %d", batch[0].ID, batch[1].ID)
	}
	for _, item := range batch {
		if err := db.QueryRow("SELECT name, value FROM ex_models WHERE id = ?", item.ID).Scan(&name, &value); err != nil {
			t.Fatalf("failed to read batch row: %v", err)
		}
		if name != item.Name || value != item.Value {
			t.Errorf("expected batch row %+v, got name=%q value=%d", item, name, value)
		}
	}
	if cache.Len() != 5 {
		t.Errorf("expected 5 cached write statements, got %d", cache.Len())
	}
}

func TestExecutor_StmtCacheAggregates(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()
	_, err = db.Exec(`
		CREATE TABLE ex_models (id INTEGER PRIMARY KEY, value INTEGER, name TEXT);
		INSERT INTO ex_models (id, value, name) VALUES (1, 10, 'A'), (2, 20, 'B'), (3, 30, 'C');
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	cache := NewStmtCache(10)
	defer cache.Close()
	ctx := context.Background()
	model := New[ExModel]().SetDB(db).WithStmtCache(cache).
		Where("value", ">", 10).OrderBy("id", "DESC").Limit(1).Offset(1)
	beforeSQL, beforeArgs := model.Print()

	for i := 0; i < 2; i++ {
		if sum, err := model.Sum(ctx, "value"); err != nil || sum != 50 {
			t.Errorf("expected sum 50, got %v, err %v", sum, err)
		}
		if avg, err := model.Avg(ctx, "value"); err != nil || avg != 25 {
			t.Errorf("expected average 25, got %v, err %v", avg, err)
		}
	}
	if cache.Len() != 2 {
		t.Errorf("repeated aggregates should reuse 2 statements, got %d", cache.Len())
	}
	afterSQL, afterArgs := model.Print()
	if beforeSQL != afterSQL || !reflect.DeepEqual(beforeArgs, afterArgs) {
		t.Error("aggregates should not change the original query")
	}
	empty := model.Clone().Where("id", 999)
	if sum, err := empty.Sum(ctx, "value"); err != nil || sum != 0 {
		t.Errorf("expected sum 0 for no matching rows, got %v, err %v", sum, err)
	}
	if avg, err := empty.Avg(ctx, "value"); err != nil || avg != 0 {
		t.Errorf("expected average 0 for no matching rows, got %v, err %v", avg, err)
	}
	if _, err := model.Sum(ctx, "value; DROP TABLE ex_models"); !errors.Is(err, ErrInvalidColumnName) {
		t.Errorf("expected invalid column from Sum, got %v", err)
	}
	if _, err := model.Avg(ctx, "value; DROP TABLE ex_models"); !errors.Is(err, ErrInvalidColumnName) {
		t.Errorf("expected invalid column from Avg, got %v", err)
	}
}

func TestExecutor_StmtCachePrepareErrors(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
	cache := NewStmtCache(10)
	defer cache.Close()
	model := New[ExModel]().SetDB(db).WithStmtCache(cache)
	ctx := context.Background()

	tests := []struct {
		name string
		run  func() error
	}{
		{"Get", func() error { _, err := model.Get(ctx); return err }},
		{"Count", func() error { _, err := model.Count(ctx); return err }},
		{"Sum", func() error { _, err := model.Sum(ctx, "value"); return err }},
		{"Avg", func() error { _, err := model.Avg(ctx, "value"); return err }},
		{"CountOver", func() error { _, err := model.CountOver(ctx, "value"); return err }},
		{"Create", func() error { return model.Create(ctx, &ExModel{Name: "A"}) }},
		{"Update", func() error { return model.Update(ctx, &ExModel{ID: 1, Name: "A"}) }},
		{"UpdateColumns", func() error { return model.UpdateColumns(ctx, &ExModel{ID: 1, Name: "A"}, "name") }},
		{"CreateMany", func() error { return model.CreateMany(ctx, []*ExModel{{Name: "A"}, {Name: "B"}}) }},
		{"Delete", func() error { return model.Clone().Where("id", 1).Delete(ctx) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run()
			if err == nil {
				t.Fatal("expected an error from the closed database")
			}
			qe := GetQueryError(err)
			if qe == nil || qe.Operation != "PREPARE" || qe.Query == "" {
				t.Errorf("expected QueryError with PREPARE context, got %v", err)
			}
		})
	}
	if cache.Len() != 0 {
		t.Errorf("failed preparations should not be cached, got %d entries", cache.Len())
	}
}
