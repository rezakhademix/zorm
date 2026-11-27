package zorm

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestCreateMany_QueryErrorOwnsPooledArguments(t *testing.T) {
	for _, operation := range []string{"CreateMany", "CreateManyNewPK"} {
		for _, mode := range []string{"query", "prepare", "sqlstate"} {
			t.Run(operation+"/"+mode, func(t *testing.T) {
				var original error = errDatabaseFailure
				if mode == "sqlstate" {
					original = &pgconn.PgError{Code: "23505", Message: "duplicate key"}
				}
				f := &failureDB{queryErr: original}
				m := New[saveItem]().SetDB(openFailureDB(t, f))
				if mode == "prepare" {
					f.prepareErr = original
					cache := NewStmtCache(10)
					defer cache.Close()
					m.WithStmtCache(cache)
				}
				run := func(name string, value int) error {
					items := []*saveItem{{Name: name, Value: value}}
					if operation == "CreateMany" {
						return m.CreateMany(context.Background(), items)
					}
					return m.CreateManyNewPK(context.Background(), items)
				}
				err := run("original", 42)
				if !errors.Is(err, original) {
					t.Fatalf("lost driver error: %v", err)
				}
				qe := GetQueryError(err)
				if qe == nil {
					t.Fatalf("missing QueryError: %v", err)
				}
				want := []any{"original", 42}
				if !reflect.DeepEqual(qe.Args, want) {
					t.Errorf("pooled args erased on return: %v", qe.Args)
				}
				_ = run("later", 99)
				if !reflect.DeepEqual(qe.Args, want) {
					t.Errorf("later pool use changed previous error: %v", qe.Args)
				}
			})
		}
	}
}
