package zorm

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

type referenceItem struct {
	ID     int `zorm:"primaryKey"`
	Count  *int
	Total  *int64
	Name   *string
	Active *bool
	At     *time.Time
	Data   []byte
}

func (referenceItem) TableName() string { return "reference_items" }

func setupReferenceDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	_, err = db.Exec(`
		CREATE TABLE reference_items (id INTEGER PRIMARY KEY, count INTEGER, total INTEGER, name TEXT, active BOOLEAN, at DATETIME, data BLOB);
		INSERT INTO reference_items VALUES (1, 10, 100, 'before', 0, '2020-01-01 00:00:00', X'616263');
		CREATE TABLE reference_writes (count INTEGER);
		INSERT INTO reference_writes VALUES (0);
		CREATE TRIGGER count_reference_updates AFTER UPDATE ON reference_items
		BEGIN UPDATE reference_writes SET count = count + 1; END;
	`)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestDirtySnapshot_PersistencePaths(t *testing.T) {
	for _, loader := range []string{"Find", "First", "Get", "Create"} {
		for _, operation := range []string{"Save", "Update", "UpdateColumns"} {
			for _, cached := range []bool{false, true} {
				name := loader + "/" + operation + "/direct"
				if cached {
					name = loader + "/" + operation + "/cached"
				}
				t.Run(name, func(t *testing.T) {
					db := setupReferenceDB(t)
					ctx := context.Background()
					m := New[referenceItem]().SetDB(db)
					if cached {
						cache := NewStmtCache(10)
						defer cache.Close()
						m = m.WithStmtCache(cache)
					}
					var entity *referenceItem
					var err error
					switch loader {
					case "Find":
						entity, err = m.Find(ctx, 1)
					case "First":
						entity, err = m.First(ctx)
					case "Get":
						var entities []*referenceItem
						entities, err = m.Get(ctx)
						if err == nil && len(entities) == 1 {
							entity = entities[0]
						}
					case "Create":
						entity = &referenceItem{Count: snapshotPtr(10), Total: snapshotPtr(int64(100)), Name: snapshotPtr("before"), Active: snapshotPtr(false), At: snapshotPtr(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)), Data: []byte("abc")}
						err = m.Create(ctx, entity)
					}
					if err != nil || entity == nil {
						t.Fatalf("load: entity=%v, err=%v", entity, err)
					}
					defer ClearOriginals(entity)
					for step := 1; step <= 2; step++ {
						*entity.Count = 10 + step
						*entity.Total = int64(100 + step)
						*entity.Name += "!"
						*entity.Active = !*entity.Active
						*entity.At = entity.At.Add(time.Hour)
						entity.Data[0]++
						if len(m.GetDirtyFields(entity)) != 6 {
							t.Fatalf("step %d: all six mutable fields should be dirty: %v", step, m.GetDirtyFields(entity))
						}
						switch operation {
						case "Save":
							err = m.Save(ctx, entity)
						case "Update":
							err = m.Update(ctx, entity)
						case "UpdateColumns":
							err = m.UpdateColumns(ctx, entity, "count", "total", "name", "active", "at", "data")
						}
						if err != nil {
							t.Fatalf("step %d: %v", step, err)
						}
						var count int
						var total int64
						var name string
						var active bool
						var at time.Time
						var data []byte
						if err := db.QueryRow(`SELECT count, total, name, active, at, data FROM reference_items WHERE id = ?`, entity.ID).Scan(&count, &total, &name, &active, &at, &data); err != nil {
							t.Fatal(err)
						}
						if count != *entity.Count || total != *entity.Total || name != *entity.Name || active != *entity.Active || !at.Equal(*entity.At) || string(data) != string(entity.Data) {
							t.Fatalf("step %d: stored values differ from entity", step)
						}
						if m.HasDirtyFields(entity) {
							t.Fatalf("step %d: successful write left dirty fields: %v", step, m.GetDirtyFields(entity))
						}
						if err := m.Save(ctx, entity); err != nil {
							t.Fatal(err)
						}
					}
					var writes int
					if err := db.QueryRow(`SELECT count FROM reference_writes`).Scan(&writes); err != nil {
						t.Fatal(err)
					}
					if writes != 2 {
						t.Errorf("expected two writes and no updates from clean Saves, got %d", writes)
					}
				})
			}
		}
	}
}

func TestDirtySnapshot_NullTransitionsPersist(t *testing.T) {
	db := setupReferenceDB(t)
	ctx := context.Background()
	m := New[referenceItem]().SetDB(db)
	entity, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(entity)
	entity.Count, entity.Total, entity.Name, entity.Active, entity.At, entity.Data = nil, nil, nil, nil, nil, nil
	if err := m.Save(ctx, entity); err != nil {
		t.Fatal(err)
	}
	var allNull bool
	if err := db.QueryRow(`SELECT count IS NULL AND total IS NULL AND name IS NULL AND active IS NULL AND at IS NULL AND data IS NULL FROM reference_items WHERE id = 1`).Scan(&allNull); err != nil || !allNull {
		t.Fatalf("expected all NULL, got %v, err=%v", allNull, err)
	}
	entity.Count, entity.Total, entity.Name, entity.Active = snapshotPtr(0), snapshotPtr(int64(0)), snapshotPtr(""), snapshotPtr(false)
	entity.At, entity.Data = snapshotPtr(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)), []byte{}
	if err := m.Save(ctx, entity); err != nil {
		t.Fatal(err)
	}
	var allPresent bool
	if err := db.QueryRow(`SELECT count = 0 AND total = 0 AND name = '' AND active = 0 AND at IS NOT NULL AND data IS NOT NULL FROM reference_items WHERE id = 1`).Scan(&allPresent); err != nil || !allPresent {
		t.Fatalf("expected non-NULL zero values, got %v, err=%v", allPresent, err)
	}
	if m.HasDirtyFields(entity) {
		t.Error("persisted zero values should be clean")
	}
}

func TestDirtySnapshot_FailedSaveCanBeRetried(t *testing.T) {
	db := setupReferenceDB(t)
	ctx := context.Background()
	m := New[referenceItem]().SetDB(db)
	entity, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(entity)
	if _, err := db.Exec(`CREATE TRIGGER reject_reference_update BEFORE UPDATE ON reference_items BEGIN SELECT RAISE(FAIL, 'blocked'); END`); err != nil {
		t.Fatal(err)
	}
	*entity.Count = 20
	entity.Data[0] = 'X'
	if err := m.Save(ctx, entity); err == nil {
		t.Fatal("expected rejected update")
	}
	if *GetOriginal(entity, "count").(*int) != 10 || string(GetOriginal(entity, "data").([]byte)) != "abc" || len(m.GetDirtyFields(entity)) != 2 {
		t.Fatalf("failed Save changed the baseline: %v", GetOriginals(entity))
	}
	if _, err := db.Exec(`DROP TRIGGER reject_reference_update`); err != nil {
		t.Fatal(err)
	}
	if err := m.Save(ctx, entity); err != nil {
		t.Fatal(err)
	}
	var count int
	var data []byte
	if err := db.QueryRow(`SELECT count, data FROM reference_items WHERE id = 1`).Scan(&count, &data); err != nil || count != 20 || string(data) != "Xbc" {
		t.Fatalf("retry lost mutation: count=%d data=%q err=%v", count, data, err)
	}
}

func TestDirtySnapshot_PartialPointerWrite(t *testing.T) {
	db := setupReferenceDB(t)
	ctx := context.Background()
	m := New[referenceItem]().SetDB(db)
	entity, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(entity)
	*entity.Count, *entity.Total = 20, 200
	if err := m.UpdateColumns(ctx, entity, "count"); err != nil {
		t.Fatal(err)
	}
	if m.IsDirtyField(entity, "count") || !m.IsDirtyField(entity, "total") || *GetOriginal(entity, "total").(*int64) != 100 {
		t.Errorf("partial pointer write lost pending total: %v", m.GetDirtyFields(entity))
	}
	*entity.Count = 30
	if *GetOriginal(entity, "count").(*int) != 20 {
		t.Error("refreshed pointer baseline aliases the entity")
	}
	if err := m.Save(ctx, entity); err != nil {
		t.Fatal(err)
	}
	var count int
	var total int64
	if err := db.QueryRow(`SELECT count, total FROM reference_items WHERE id = 1`).Scan(&count, &total); err != nil || count != 30 || total != 200 {
		t.Fatalf("pending pointer changes lost: count=%d total=%d err=%v", count, total, err)
	}
}
