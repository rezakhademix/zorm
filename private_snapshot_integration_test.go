package zorm

import (
	"context"
	"database/sql/driver"
	"fmt"
	"testing"
)

type privateSnapshotValue struct {
	data   []byte
	labels map[string][]byte
	count  *int
}

func (v *privateSnapshotValue) Scan(value any) error {
	switch value := value.(type) {
	case []byte:
		v.data = append([]byte(nil), value...)
	case string:
		v.data = []byte(value)
	default:
		return fmt.Errorf("unsupported value %T", value)
	}
	return nil
}

func (v privateSnapshotValue) Value() (driver.Value, error) { return v.data, nil }

type privateSnapshotRecord struct {
	ID   int
	Data privateSnapshotValue
}

func (privateSnapshotRecord) TableName() string { return "private_snapshot_records" }

func TestPrivateSnapshot_DetachesReferences(t *testing.T) {
	value := privateSnapshotValue{data: []byte("abc"), labels: map[string][]byte{"key": []byte("abc")}, count: snapshotPtr(1)}
	original := snapshotValue(value).(privateSnapshotValue)
	value.data[0] = 'z'
	value.labels["key"][0] = 'z'
	*value.count = 2
	if string(original.data) != "abc" || string(original.labels["key"]) != "abc" || *original.count != 1 {
		t.Fatal("private fields share mutable storage with the snapshot")
	}
}

func TestPrivateSnapshot_SavePersistsMutation(t *testing.T) {
	for _, mode := range []string{"direct", "cached", "transaction"} {
		t.Run(mode, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			if _, err := db.Exec(`CREATE TABLE private_snapshot_records (id INTEGER PRIMARY KEY, data BLOB);
				INSERT INTO private_snapshot_records VALUES (1, X'616263')`); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			m := New[privateSnapshotRecord]().SetDB(db)
			if mode == "cached" {
				cache := NewStmtCache(10)
				defer cache.Close()
				m.WithStmtCache(cache)
			}
			write := func(q *Model[privateSnapshotRecord]) error {
				entity, err := q.Find(ctx, 1)
				if err != nil {
					return err
				}
				defer ClearOriginals(entity)
				entity.Data.data[0] = 'z'
				if !q.IsDirtyField(entity, "data") {
					t.Error("private scanner mutation was not detected")
				}
				original := GetOriginal(entity, "data").(privateSnapshotValue)
				original.data[0] = 'x'
				if got := GetOriginal(entity, "data").(privateSnapshotValue); string(got.data) != "abc" {
					t.Error("reading or mutating an exposed original changed the baseline")
				}
				return q.Save(ctx, entity)
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
			var data []byte
			if err := db.QueryRow(`SELECT data FROM private_snapshot_records WHERE id = 1`).Scan(&data); err != nil {
				t.Fatal(err)
			}
			if string(data) != "zbc" {
				t.Fatalf("private scanner edit was not persisted: %q", data)
			}
		})
	}
}
