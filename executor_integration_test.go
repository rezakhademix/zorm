package zorm

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

type ExModel struct {
	ID    int `zorm:"primaryKey"`
	Value int
	Name  string
}

func (m ExModel) TableName() string { return "ex_models" }

func setupExDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE ex_models (id INTEGER PRIMARY KEY, value INTEGER, name TEXT);
		INSERT INTO ex_models (value, name) VALUES (10, 'A'), (20, 'B'), (30, 'C');
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

func TestExecutor_Aggregates(t *testing.T) {
	db := setupExDB(t)
	defer db.Close()

	m := New[ExModel]().SetDB(db)
	ctx := context.Background()

	// Sum
	sum, err := m.Sum(ctx, "value")
	if err != nil || sum != 60 {
		t.Errorf("Sum failed: got %f, err %v", sum, err)
	}

	// Avg
	avg, err := m.Avg(ctx, "value")
	if err != nil || avg != 20 {
		t.Errorf("Avg failed: got %f, err %v", avg, err)
	}

	// Pluck
	names, err := m.OrderBy("id", "ASC").Pluck(ctx, "name")
	if err != nil || len(names) != 3 {
		t.Fatalf("Pluck failed: %v", err)
	}
	if names[0] != "A" || names[1] != "B" || names[2] != "C" {
		t.Errorf("Pluck values mismatch: %v", names)
	}
}

func TestExecutor_FindOrFail(t *testing.T) {
	db := setupExDB(t)
	defer db.Close()

	m := New[ExModel]().SetDB(db)
	ctx := context.Background()

	// Exists
	res, err := m.FindOrFail(ctx, 1)
	if err != nil || res.Name != "A" {
		t.Errorf("FindOrFail(1) failed: %v", err)
	}

	// Not exists
	_, err = m.FindOrFail(ctx, 999)
	if err == nil {
		t.Error("expected error for non-existent ID in FindOrFail")
	}
	if !errors.Is(err, ErrRecordNotFound) {
		t.Errorf("expected ErrRecordNotFound, got %v", err)
	}
}

func TestExecutor_Cursor(t *testing.T) {
	db := setupExDB(t)
	defer db.Close()

	m := New[ExModel]().SetDB(db).OrderBy("id", "ASC")
	ctx := context.Background()

	cursor, err := m.Cursor(ctx)
	if err != nil {
		t.Fatalf("Cursor failed: %v", err)
	}
	defer cursor.Close()

	count := 0
	for cursor.Next() {
		item, err := cursor.Scan()
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		count++
		if count == 1 && item.Name != "A" {
			t.Errorf("expected A, got %s", item.Name)
		}
	}

	if count != 3 {
		t.Errorf("expected 3 items via cursor, got %d", count)
	}
}

func TestExecutor_FirstOrCreate(t *testing.T) {
	db := setupExDB(t)
	defer db.Close()

	m := New[ExModel]().SetDB(db)
	ctx := context.Background()

	// Create new
	item, err := m.FirstOrCreate(ctx,
		map[string]any{"name": "D"},
		map[string]any{"value": 40},
	)
	if err != nil || item.Value != 40 {
		t.Errorf("FirstOrCreate (create) failed: err=%v, value=%d", err, item.Value)
	}

	// Find existing
	item2, err := m.FirstOrCreate(ctx,
		map[string]any{"name": "A"},
		nil,
	)
	if err != nil || item2.Value != 10 {
		t.Errorf("FirstOrCreate (find) failed: err=%v, value=%d", err, item2.Value)
	}
}
