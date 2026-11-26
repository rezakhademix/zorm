package zorm

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

func TestPagination_DefaultsAndReadFailures(t *testing.T) {
	db := setupExDB(t)
	defer db.Close()
	ctx := context.Background()
	m := New[ExModel]().SetDB(db)
	page, err := m.Paginate(ctx, 0, 0)
	if err != nil || page.CurrentPage != 1 || page.PerPage != 15 || page.Total != 3 || len(page.Data) != 3 {
		t.Fatalf("invalid pagination defaults: %+v err=%v", page, err)
	}
	simple, err := m.SimplePaginate(ctx, -1, -1)
	if err != nil || simple.CurrentPage != 1 || simple.PerPage != 15 || simple.Total != -1 || len(simple.Data) != 3 {
		t.Fatalf("invalid simple pagination defaults: %+v err=%v", simple, err)
	}
	for _, failCount := range []bool{false, true} {
		failure := &failureDB{query: func(query string) (driver.Rows, error) {
			if !failCount && strings.Contains(query, "COUNT(*)") {
				return &failureRows{columns: []string{"count"}, values: [][]driver.Value{{int64(3)}}}, nil
			}
			return nil, errDatabaseFailure
		}}
		failed := New[ExModel]().SetDB(openFailureDB(t, failure))
		if _, err := failed.Paginate(ctx, 1, 2); !errors.Is(err, errDatabaseFailure) {
			t.Errorf("pagination lost database error: %v", err)
		}
		if _, err := failed.SimplePaginate(ctx, 1, 2); !errors.Is(err, errDatabaseFailure) {
			t.Errorf("simple pagination lost database error: %v", err)
		}
	}
}

func TestChunk_CallbackAndQueryFailures(t *testing.T) {
	ctx := context.Background()
	db := setupExDB(t)
	defer db.Close()
	m := New[ExModel]().SetDB(db)
	called := 0
	err := m.Chunk(ctx, 1, func(rows []*ExModel) error {
		called++
		if len(rows) != 1 {
			t.Errorf("expected one row, got %d", len(rows))
		}
		return errDatabaseFailure
	})
	if !errors.Is(err, errDatabaseFailure) || called != 1 || m.limit != 0 || m.offset != 0 {
		t.Errorf("callback failure did not stop chunking or mutated builder: calls=%d err=%v", called, err)
	}
	failed := New[ExModel]().SetDB(openFailureDB(t, &failureDB{queryErr: errDatabaseFailure}))
	if err := failed.Chunk(ctx, 1, func([]*ExModel) error { t.Error("callback called after query failure"); return nil }); !errors.Is(err, errDatabaseFailure) {
		t.Errorf("chunking lost query failure: %v", err)
	}
}

func TestScalarFailures_FirstAndCount(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"query", "iteration", "conversion", "empty"} {
		t.Run(mode, func(t *testing.T) {
			failure := &failureDB{columns: []string{"value"}}
			switch mode {
			case "query":
				failure.queryErr = errDatabaseFailure
			case "iteration":
				failure.iterationErr = errDatabaseFailure
			case "conversion":
				failure.values = [][]driver.Value{{"bad integer"}}
			}
			q := Query[int]().SetDB(openFailureDB(t, failure)).Table("ex_models").Select("value")
			_, err := q.First(ctx)
			if err == nil || mode == "empty" && !errors.Is(err, ErrRecordNotFound) {
				t.Fatalf("unexpected First result: %v", err)
			}
			if (mode == "query" || mode == "iteration") && !errors.Is(err, errDatabaseFailure) {
				t.Errorf("First lost original error: %v", err)
			}
			if _, err := q.Count(ctx); err == nil {
				t.Error("Count accepted invalid or absent result")
			}
		})
	}
}

func TestDistinctOnCount_UsesSubquery(t *testing.T) {
	original := GetDialect()
	SetDialect(DialectPostgres)
	defer SetDialect(original)
	var received string
	db := openFailureDB(t, &failureDB{query: func(query string) (driver.Rows, error) {
		received = query
		return &failureRows{columns: []string{"count"}, values: [][]driver.Value{{int64(2)}}}, nil
	}})
	count, err := New[ExModel]().SetDB(db).DistinctBy("name").Where("value", ">", 5).Count(context.Background())
	if err != nil || count != 2 || !strings.Contains(received, "SELECT COUNT(*) FROM (SELECT DISTINCT ON (name)") || !strings.Contains(received, "value > $1") {
		t.Fatalf("wrong DISTINCT ON count: count=%d query=%q err=%v", count, received, err)
	}
}

func TestScalarValidation_NonBinaryAndOversizedConditions(t *testing.T) {
	for _, operator := range []string{"BETWEEN", "IN", "NOT IN"} {
		q := Query[int]().Where("id", operator, 1)
		if q.buildErr == nil {
			t.Errorf("scalar Where accepted non-binary operator %q", operator)
		}
	}
	if q := Query[int]().Where("id", "=", 1, 2); q.buildErr == nil {
		t.Error("scalar Where accepted extra arguments")
	}
	if q := Query[int]().WhereIn("id;delete", []any{1}); !errors.Is(q.buildErr, ErrInvalidColumnName) {
		t.Errorf("invalid IN column was accepted: %v", q.buildErr)
	}
	values := make([]any, maxInArgs+1)
	for i := range values {
		values[i] = i
	}
	original := GetDialect()
	SetDialect(DialectSQLite)
	defer SetDialect(original)
	if q := Query[int]().WhereIn("id", values); q.buildErr == nil {
		t.Error("scalar IN accepted too many parameters")
	}
	if m := New[ExModel]().OrWhereIn("id", values); m.buildErr == nil {
		t.Error("OR IN accepted too many parameters")
	}
}
