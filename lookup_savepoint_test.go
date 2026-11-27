package zorm

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// PostgreSQL rejects every statement after a constraint failure until rollback.
func TestLookupCreate_DuplicateDoesNotAbortTransaction(t *testing.T) {
	for _, operation := range []string{"FirstOrCreate", "UpdateOrCreate"} {
		t.Run(operation, func(t *testing.T) {
			aborted, inserted, protected, recovered := false, false, false, false
			f := &failureDB{affected: 1}
			f.onExec = func(query string) {
				if strings.HasPrefix(query, "SAVEPOINT ") {
					protected = true
				}
				if strings.HasPrefix(query, "ROLLBACK TO SAVEPOINT ") && protected {
					aborted, recovered = false, true
				}
				if aborted {
					f.execErr = &pgconn.PgError{Code: "25P02"}
				} else {
					f.execErr = nil
				}
			}
			f.query = func(query string) (driver.Rows, error) {
				if aborted {
					return nil, &pgconn.PgError{Code: "25P02"}
				}
				if strings.HasPrefix(query, "INSERT ") {
					inserted, aborted = true, true
					return nil, &pgconn.PgError{Code: "23505"}
				}
				rows := &failureRows{columns: []string{"id", "name", "value"}}
				if inserted {
					rows.values = [][]driver.Value{{int64(1), "winner", int64(10)}}
				}
				return rows, nil
			}
			db := openFailureDB(t, f)
			m := New[saveItem]().SetDB(db)
			m.dialect = DialectPostgres
			err := m.Transaction(context.Background(), func(tx *Tx) error {
				q := m.WithTx(tx)
				var item *saveItem
				var err error
				if operation == "FirstOrCreate" {
					item, err = q.FirstOrCreate(context.Background(), map[string]any{"name": "winner"}, nil)
				} else {
					item, err = q.UpdateOrCreate(context.Background(), map[string]any{"name": "winner"}, map[string]any{"value": 20})
				}
				if err != nil {
					return err
				}
				defer ClearOriginals(item)
				if item.ID != 1 {
					t.Errorf("wrong winning row: %+v", item)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if !recovered || aborted {
				t.Error("duplicate did not recover the transaction")
			}
		})
	}
}

type lookupHookKey struct{}
type lookupHookRecord struct {
	ID   int
	Name string
}

func (lookupHookRecord) TableName() string { return "save_items" }
func (*lookupHookRecord) BeforeCreateTx(ctx context.Context, tx *Tx) error {
	return ctx.Value(lookupHookKey{}).(func(*Tx) error)(tx)
}
func (*lookupHookRecord) AfterCreateTx(context.Context, *Tx) error { return errDatabaseFailure }

func TestLookupCreate_SavepointRestoresHookBaselines(t *testing.T) {
	for _, managed := range []bool{false, true} {
		db := setupSaveDB(t)
		ctx := context.Background()
		var item *saveVersionedItem
		run := func(tx *Tx) error {
			m := New[saveVersionedItem]().SetDB(db).WithTx(tx)
			var err error
			item, err = m.Find(ctx, 1)
			if err != nil {
				return err
			}
			defer ClearOriginals(item)
			item.Name = "pending"
			hookCtx := context.WithValue(ctx, lookupHookKey{}, func(hookTx *Tx) error {
				return New[saveVersionedItem]().SetDB(db).WithTx(hookTx).Save(ctx, item)
			})
			q := New[lookupHookRecord]().SetDB(db).WithTx(tx)
			q.dialect = DialectPostgres
			_, err = q.FirstOrCreate(hookCtx, map[string]any{"name": "new"}, nil)
			if !errors.Is(err, errDatabaseFailure) {
				t.Fatalf("lost hook error: %v", err)
			}
			if item.Version != 1 || GetOriginal(item, "name") != "A" {
				t.Errorf("savepoint retained hook baseline: %+v", item)
			}
			var version int
			if err := tx.Tx.QueryRow("SELECT version FROM save_versioned_items WHERE id = 1").Scan(&version); err != nil {
				return err
			}
			if version != 1 {
				t.Errorf("hook write survived rollback: %d", version)
			}
			if !managed {
				if _, ok := globalTracker.Load().LoadForTransaction(getEntityKey(item), item, nil); ok {
					t.Error("caller-owned baseline lost its transaction binding")
				}
			}
			return m.Save(ctx, item)
		}
		var err error
		if managed {
			err = New[saveItem]().SetDB(db).Transaction(ctx, run)
		} else {
			tx, beginErr := db.BeginTx(ctx, nil)
			if beginErr != nil {
				t.Fatal(beginErr)
			}
			err = run(&Tx{Tx: tx, ctx: ctx})
			if err == nil {
				err = tx.Commit()
			} else {
				_ = tx.Rollback()
			}
		}
		db.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestLookupCreate_SuccessPreservesOuterTransactionTracking(t *testing.T) {
	for _, managed := range []bool{false, true} {
		db := setupSaveDB(t)
		ctx := context.Background()
		m := New[saveItem]().SetDB(db)
		m.dialect = DialectPostgres
		var item *saveItem
		run := func(tx *Tx) error {
			var err error
			item, err = m.WithTx(tx).FirstOrCreate(ctx, map[string]any{"name": "new"}, map[string]any{"value": 30})
			return err
		}
		if managed {
			err := m.Transaction(ctx, func(tx *Tx) error {
				if err := run(tx); err != nil {
					return err
				}
				return errDatabaseFailure
			})
			if !errors.Is(err, errDatabaseFailure) {
				t.Fatal(err)
			}
			if IsTracked(item) {
				t.Error("outer rollback retained successful savepoint baseline")
			}
		} else {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := run(&Tx{Tx: tx, ctx: ctx}); err != nil {
				_ = tx.Rollback()
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			item.Value = 40
			if err := m.Save(ctx, item); !errors.Is(err, ErrSaveUntracked) {
				t.Errorf("caller-owned transaction baseline escaped its binding: %v", err)
			}
		}
		ClearOriginals(item)
		db.Close()
	}
}
