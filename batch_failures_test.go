package zorm

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

func TestBatchFailures_TransactionBoundaries(t *testing.T) {
	ctx := context.Background()
	for _, batch := range []string{"mixed keys", "multiple chunks"} {
		for _, mode := range []string{"begin", "query", "commit", "rollback"} {
			t.Run(batch+"/"+mode, func(t *testing.T) {
				failure := &failureDB{columns: []string{"id"}}
				switch mode {
				case "begin":
					failure.beginErr = errDatabaseFailure
				case "query":
					failure.queryErr = errDatabaseFailure
				case "rollback":
					failure.queryErr, failure.rollbackErr = errDatabaseFailure, errDatabaseFailure
				case "commit":
					failure.commitErr = errDatabaseFailure
					failure.query = func(query string) (driver.Rows, error) {
						count := strings.Count(query, "(") - 1
						rows := &failureRows{columns: []string{"id"}}
						for i := 0; i < count; i++ {
							rows.values = append(rows.values, []driver.Value{int64(i + 1)})
						}
						return rows, nil
					}
				}
				entities := []*ExModel{{Name: "auto"}, {ID: 10, Name: "explicit"}}
				if batch == "multiple chunks" {
					entities = make([]*ExModel, bulkInsertMaxRowsPerStmt+1)
					for i := range entities {
						entities[i] = &ExModel{Name: "row"}
					}
				}
				m := New[ExModel]().SetDB(openFailureDB(t, failure))
				err := m.CreateMany(ctx, entities)
				if !errors.Is(err, errDatabaseFailure) {
					t.Fatalf("batch lost transaction or driver failure: %v", err)
				}
				if batch == "mixed keys" && mode == "rollback" && !errors.Is(err, ErrRollbackFailed) {
					t.Errorf("rollback failure lost: %v", err)
				}
			})
		}
	}
}

func TestBatchFailures_ExistingTransactionAndScanFailures(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"query", "scan", "iteration"} {
		t.Run(mode, func(t *testing.T) {
			failure := &failureDB{columns: []string{"id"}}
			switch mode {
			case "query":
				failure.queryErr = errDatabaseFailure
			case "scan":
				failure.values = [][]driver.Value{{"bad id"}}
			case "iteration":
				failure.iterationErr = errDatabaseFailure
			}
			db := openFailureDB(t, failure)
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			m := New[ExModel]().SetDB(db).WithTx(&Tx{Tx: tx, ctx: ctx})
			err = m.CreateMany(ctx, []*ExModel{{Name: "auto"}, {ID: 10, Name: "explicit"}})
			if err == nil || mode != "scan" && !errors.Is(err, errDatabaseFailure) {
				t.Errorf("batch accepted invalid driver results: %v", err)
			}
		})
	}
}

func TestSaveFailures_RowsAffectedAndExistenceProbe(t *testing.T) {
	ctx := context.Background()
	t.Run("row count unavailable", func(t *testing.T) {
		m := New[ExModel]().SetDB(openFailureDB(t, &failureDB{affectedErr: errDatabaseFailure}))
		entity := &ExModel{ID: 1, Name: "before"}
		trackOriginals(entity, m.modelInfo)
		defer ClearOriginals(entity)
		entity.Name = "after"
		if err := m.Save(ctx, entity); !errors.Is(err, errDatabaseFailure) {
			t.Errorf("row count failure lost: %v", err)
		}
		if GetOriginal(entity, "name") != "before" || !m.HasDirtyFields(entity) {
			t.Error("unconfirmed write advanced tracking baseline")
		}
	})
	for _, deleted := range []bool{false, true} {
		failure := &failureDB{columns: []string{"id"}}
		want := ErrRecordNotFound
		if !deleted {
			failure.queryErr = errDatabaseFailure
			want = errDatabaseFailure
		}
		m := New[saveVersionedItem]().SetDB(openFailureDB(t, failure))
		entity := &saveVersionedItem{ID: 1, Name: "before", Version: 1}
		trackOriginals(entity, m.modelInfo)
		defer ClearOriginals(entity)
		entity.Name = "after"
		if err := m.Save(ctx, entity); !errors.Is(err, want) {
			t.Errorf("existence probe returned %v, want %v", err, want)
		}
		if entity.Version != 1 || GetOriginal(entity, "name") != "before" {
			t.Error("failed optimistic update advanced version or baseline")
		}
	}
}

func TestTransactionFailures_BeginAndCommit(t *testing.T) {
	for _, mode := range []string{"begin", "commit"} {
		failure := &failureDB{}
		if mode == "begin" {
			failure.beginErr = errDatabaseFailure
		} else {
			failure.commitErr = errDatabaseFailure
		}
		called := false
		err := New[ExModel]().SetDB(openFailureDB(t, failure)).Transaction(context.Background(), func(*Tx) error { called = true; return nil })
		if !errors.Is(err, errDatabaseFailure) || called != (mode == "commit") {
			t.Errorf("transaction failed incorrectly: mode=%s callback=%v err=%v", mode, called, err)
		}
	}
}
