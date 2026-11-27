package zorm

import (
	"context"
	"database/sql/driver"
	"errors"
	"reflect"
	"testing"
)

type scalarAggregateOperation struct {
	name string
	run  func(*Model[saveItem]) (float64, error)
}

func scalarAggregateOperations(ctx context.Context) []scalarAggregateOperation {
	return []scalarAggregateOperation{
		{"Count", func(m *Model[saveItem]) (float64, error) { n, err := m.Count(ctx); return float64(n), err }},
		{"Sum", func(m *Model[saveItem]) (float64, error) { return m.Sum(ctx, "value") }},
		{"Avg", func(m *Model[saveItem]) (float64, error) { return m.Avg(ctx, "value") }},
		{"Min", func(m *Model[saveItem]) (float64, error) { return m.Min(ctx, "value") }},
		{"Max", func(m *Model[saveItem]) (float64, error) { return m.Max(ctx, "value") }},
	}
}

func TestScalarAggregates_RejectGroupedQueriesBeforeSQL(t *testing.T) {
	ctx := context.Background()
	for _, op := range scalarAggregateOperations(ctx) {
		for _, shape := range []string{"group", "having", "both"} {
			for _, mode := range []string{"direct", "cached", "transaction"} {
				t.Run(op.name+"/"+shape+"/"+mode, func(t *testing.T) {
					queries := 0
					f := &failureDB{query: func(string) (driver.Rows, error) {
						queries++
						return &failureRows{columns: []string{"aggregate"}, values: [][]driver.Value{{int64(1)}}}, nil
					}}
					db := openFailureDB(t, f)
					m := New[saveItem]().SetDB(db).Select("name").Where("value", ">", 0).OrderBy("name", "ASC").Limit(2).Offset(1)
					if shape != "having" {
						m.GroupBy("name")
					}
					if shape != "group" {
						m.Having("COUNT(*) > ?", 0)
					}
					cache := NewStmtCache(10)
					defer cache.Close()
					if mode == "cached" {
						m.WithStmtCache(cache)
					}
					run := func(q *Model[saveItem]) error {
						before, beforeArgs := q.Print()
						_, err := op.run(q)
						if !errors.Is(err, ErrGroupedAggregateRequiresGet) {
							t.Errorf("expected grouped aggregate sentinel, got %v", err)
						}
						after, afterArgs := q.Print()
						if before != after || !reflect.DeepEqual(beforeArgs, afterArgs) {
							t.Error("aggregate changed query state")
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
						t.Errorf("rejected aggregate executed %d queries, cache size %d", queries, cache.Len())
					}
				})
			}
		}
	}
}

func TestScalarCount_RejectsGroupedQueriesBeforeSQL(t *testing.T) {
	for _, shape := range []string{"group", "having", "both"} {
		t.Run(shape, func(t *testing.T) {
			queries := 0
			f := &failureDB{query: func(string) (driver.Rows, error) {
				queries++
				return &failureRows{columns: []string{"aggregate"}, values: [][]driver.Value{{int64(1)}}}, nil
			}}
			q := Query[int]().SetDB(openFailureDB(t, f)).Table("save_items").Select("value").Where("id", ">", 0).Limit(2).Offset(1)
			if shape != "having" {
				q.GroupBy("value")
			}
			if shape != "group" {
				q.Having("COUNT(*) > ?", 0)
			}
			before, beforeArgs := q.Print()
			if _, err := q.Count(context.Background()); !errors.Is(err, ErrGroupedAggregateRequiresGet) {
				t.Errorf("expected grouped aggregate sentinel, got %v", err)
			}
			after, afterArgs := q.Print()
			if queries != 0 {
				t.Errorf("rejected Count executed %d queries", queries)
			}
			if before != after || !reflect.DeepEqual(beforeArgs, afterArgs) {
				t.Error("Count changed query state")
			}
		})
	}
}

func TestScalarAggregates_UngroupedBehavior(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"direct", "cached", "transaction"} {
		t.Run(mode, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			m := New[saveItem]().SetDB(db).Where("id", ">", 0).OrderBy("name", "ASC").Limit(1).Offset(1)
			if mode == "cached" {
				cache := NewStmtCache(10)
				defer cache.Close()
				m.WithStmtCache(cache)
			}
			run := func(q *Model[saveItem]) error {
				want := map[string]float64{"Count": 2, "Sum": 30, "Avg": 15, "Min": 10, "Max": 20}
				for _, op := range scalarAggregateOperations(ctx) {
					before, beforeArgs := q.Print()
					n, err := op.run(q)
					if err != nil {
						return err
					}
					if n != want[op.name] {
						t.Errorf("%s=%v, want %v", op.name, n, want[op.name])
					}
					n, err = op.run(q.Clone().Where("id", 999))
					if err != nil || n != 0 {
						t.Errorf("empty %s=%v, err=%v", op.name, n, err)
					}
					after, afterArgs := q.Print()
					if before != after || !reflect.DeepEqual(beforeArgs, afterArgs) {
						t.Errorf("%s changed query state", op.name)
					}
				}
				if _, err := q.queryerForWrite().ExecContext(ctx, "UPDATE save_items SET value = NULL"); err != nil {
					return err
				}
				for _, op := range scalarAggregateOperations(ctx) {
					n, err := op.run(q)
					expected := float64(0)
					if op.name == "Count" {
						expected = 2
					}
					if err != nil || n != expected {
						t.Errorf("NULL %s=%v, err=%v", op.name, n, err)
					}
				}
				return nil
			}
			var err error
			if mode == "transaction" {
				err = m.Transaction(ctx, func(tx *Tx) error { return run(m.WithTx(tx)) })
			} else {
				err = run(m)
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

type groupedAggregateReport struct {
	Name    string
	Count   int
	Total   float64
	Average float64
	Minimum float64
	Maximum float64
}

func TestScalarAggregates_ExplicitGroupedGet(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	if _, err := db.Exec(`INSERT INTO save_items (name, value) VALUES ('A', 30), ('C', 1000)`); err != nil {
		t.Fatal(err)
	}
	q := New[groupedAggregateReport]().SetDB(db).Table("save_items").
		Select("name", "COUNT(*) AS count", "SUM(value) AS total", "AVG(value) AS average", "MIN(value) AS minimum", "MAX(value) AS maximum").
		GroupBy("name").Having("COUNT(*) > ?", 1)
	rows, err := q.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		defer ClearOriginals(row)
	}
	want := &groupedAggregateReport{Name: "A", Count: 2, Total: 40, Average: 20, Minimum: 10, Maximum: 30}
	if len(rows) != 1 || !reflect.DeepEqual(rows[0], want) {
		t.Fatalf("grouped aggregates=%+v, want %+v", rows, want)
	}
}

func TestScalarAggregates_MinMaxValidateColumns(t *testing.T) {
	m := New[saveItem]()
	for _, column := range []string{"value; DROP TABLE save_items", "value -- comment"} {
		if _, err := m.Min(context.Background(), column); err == nil {
			t.Error("Min accepted invalid column")
		}
		if _, err := m.Max(context.Background(), column); err == nil {
			t.Error("Max accepted invalid column")
		}
	}
}
