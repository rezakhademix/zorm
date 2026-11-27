package zorm

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
)

type nullableScannerRecord struct {
	ID   int
	Name *sql.NullString
}

func (nullableScannerRecord) TableName() string { return "nullable_scanner_records" }

// Pointer scanners are database columns, including SQL NULL and in-place edits.
func TestNullableScanner_Persistence(t *testing.T) {
	for _, mode := range []string{"direct", "cached", "transaction"} {
		t.Run(mode, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			if _, err := db.Exec(`CREATE TABLE nullable_scanner_records (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			m := New[nullableScannerRecord]().SetDB(db)
			if mode == "cached" {
				cache := NewStmtCache(10)
				defer cache.Close()
				m.WithStmtCache(cache)
			}
			write := func(q *Model[nullableScannerRecord]) error {
				entity := &nullableScannerRecord{Name: &sql.NullString{String: "created", Valid: true}}
				if err := q.Create(ctx, entity); err != nil {
					return err
				}
				defer ClearOriginals(entity)
				loaded, err := q.Find(ctx, entity.ID)
				if err != nil {
					return err
				}
				defer ClearOriginals(loaded)
				if loaded.Name == nil || !loaded.Name.Valid || loaded.Name.String != "created" {
					return fmt.Errorf("pointer scanner was omitted: %+v", loaded)
				}
				loaded.Name.String = "saved"
				if err := q.Save(ctx, loaded); err != nil {
					return err
				}
				var name string
				if err := q.queryer().QueryRowContext(ctx, `SELECT name FROM nullable_scanner_records WHERE id = ?`, loaded.ID).Scan(&name); err != nil {
					return err
				}
				if name != "saved" {
					t.Errorf("scanner mutation was not saved: %q", name)
				}
				loaded.Name = nil
				if err := q.UpdateColumns(ctx, loaded, "name"); err != nil {
					return err
				}
				empty, err := q.Find(ctx, loaded.ID)
				if err != nil {
					return err
				}
				defer ClearOriginals(empty)
				if empty.Name != nil {
					t.Error("SQL NULL did not load as a nil scanner pointer")
				}
				return nil
			}
			var err error
			if mode == "transaction" {
				err = m.Transaction(ctx, func(tx *Tx) error { return write(m.WithTx(tx)) })
			} else {
				err = write(m)
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
