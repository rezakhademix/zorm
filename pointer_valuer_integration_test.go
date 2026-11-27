package zorm

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
)

type pointerDatabaseValue struct{ text string }

func (v *pointerDatabaseValue) Scan(value any) error {
	switch value := value.(type) {
	case string:
		v.text = value
	case []byte:
		v.text = string(value)
	default:
		return fmt.Errorf("unsupported value %T", value)
	}
	return nil
}
func (v *pointerDatabaseValue) Value() (driver.Value, error) {
	if v.text == "error" {
		return nil, errDatabaseFailure
	}
	return v.text, nil
}

type pointerValuerRecord struct {
	ID   int
	Data pointerDatabaseValue
}

func (pointerValuerRecord) TableName() string { return "pointer_valuer_records" }

func TestPointerValuer_AllEntityWrites(t *testing.T) {
	for _, operation := range []string{"Create", "CreateMany", "BulkInsert", "Update", "UpdateColumns", "Save"} {
		for _, mode := range []string{"direct", "cached", "transaction"} {
			t.Run(operation+"/"+mode, func(t *testing.T) {
				db := setupSaveDB(t)
				defer db.Close()
				if _, err := db.Exec(`CREATE TABLE pointer_valuer_records (id INTEGER PRIMARY KEY, data TEXT);
					INSERT INTO pointer_valuer_records VALUES (1, 'old')`); err != nil {
					t.Fatal(err)
				}
				ctx := context.Background()
				m := New[pointerValuerRecord]().SetDB(db)
				if mode == "cached" {
					cache := NewStmtCache(10)
					defer cache.Close()
					m.WithStmtCache(cache)
				}
				run := func(q *Model[pointerValuerRecord]) error {
					item, err := q.Find(ctx, 1)
					if err != nil {
						return err
					}
					defer ClearOriginals(item)
					item.Data.text = "next"
					switch operation {
					case "Create":
						item.ID = 0
						err = q.Create(ctx, item)
					case "CreateMany":
						item.ID = 0
						err = q.CreateMany(ctx, []*pointerValuerRecord{item})
					case "BulkInsert":
						item.ID = 0
						err = q.BulkInsert(ctx, []*pointerValuerRecord{item})
					case "Update":
						err = q.Update(ctx, item)
					case "UpdateColumns":
						err = q.UpdateColumns(ctx, item, "data")
					case "Save":
						err = q.Save(ctx, item)
					}
					if err != nil {
						return err
					}
					var text string
					if err := q.queryer().QueryRowContext(ctx, "SELECT data FROM pointer_valuer_records WHERE id = ?", item.ID).Scan(&text); err != nil {
						return err
					}
					if text != "next" {
						t.Errorf("stored %q, want next", text)
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
}

func TestPointerValuer_SaveFailurePreservesBaseline(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE pointer_valuer_records (id INTEGER PRIMARY KEY, data TEXT);
		INSERT INTO pointer_valuer_records VALUES (1, 'old')`); err != nil {
		t.Fatal(err)
	}
	m := New[pointerValuerRecord]().SetDB(db)
	item, err := m.Find(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(item)
	item.Data.text = "error"
	if err := m.Save(context.Background(), item); !errors.Is(err, errDatabaseFailure) {
		t.Fatalf("lost Value error: %v", err)
	}
	if original := GetOriginal(item, "data").(pointerDatabaseValue); original.text != "old" {
		t.Error("failed Value call changed baseline")
	}
}
