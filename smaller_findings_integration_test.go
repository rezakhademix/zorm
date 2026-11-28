package zorm

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestChunk_CallbackDeletesDoNotSkipRows(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	ctx := context.Background()
	seen := []int{}
	err := New[saveItem]().SetDB(db).Chunk(ctx, 1, func(rows []*saveItem) error {
		for _, row := range rows {
			seen = append(seen, row.ID)
			defer ClearOriginals(row)
			if _, err := db.Exec("DELETE FROM save_items WHERE id = ?", row.ID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(seen, []int{1, 2}) {
		t.Fatalf("chunk skipped rows: %v", seen)
	}
}

type createManyHookRecord struct {
	ID   int
	Name string
}

func (createManyHookRecord) TableName() string { return "save_items" }
func (r *createManyHookRecord) BeforeCreate(context.Context) error {
	if r.Name == "invalid" {
		return ErrInvalidModel
	}
	r.Name = "hooked"
	return nil
}
func TestCreateMany_RunsBeforeCreate(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	m := New[createManyHookRecord]().SetDB(db)
	if err := m.CreateMany(context.Background(), []*createManyHookRecord{{Name: "invalid"}}); !errors.Is(err, ErrInvalidModel) {
		t.Errorf("validation hook bypassed: %v", err)
	}
	entity := &createManyHookRecord{Name: "valid"}
	if err := m.CreateMany(context.Background(), []*createManyHookRecord{entity}); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := db.QueryRow("SELECT name FROM save_items WHERE id = ?", entity.ID).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "hooked" {
		t.Errorf("hook mutation not stored: %s", name)
	}
}

func TestDirtyTracker_DoesNotEvictBeforeCapacity(t *testing.T) {
	tracker := newLRUTracker(1000)
	for i := uintptr(1); i <= 1000; i++ {
		tracker.Store(i, map[string]any{"id": i}, nil)
	}
	if tracker.Len() != 1000 {
		t.Fatalf("evicted before capacity: %d entries", tracker.Len())
	}
}

func TestNilDatabase_ReturnsError(t *testing.T) {
	oldDB, oldResolver := GetGlobalDB(), GetGlobalResolver()
	SetGlobalDB(nil)
	defer SetGlobalDB(oldDB)
	defer globalResolver.Store(oldResolver)
	for _, resolver := range []*DBResolver{nil, {}} {
		globalResolver.Store(resolver)
		for name, run := range map[string]func() error{
			"Count":     func() error { _, err := New[saveItem]().Count(context.Background()); return err },
			"Exists":    func() error { _, err := New[saveItem]().Exists(context.Background()); return err },
			"Sum":       func() error { _, err := New[saveItem]().Sum(context.Background(), "value"); return err },
			"Pluck":     func() error { _, err := New[saveItem]().Pluck(context.Background(), "name"); return err },
			"CountOver": func() error { _, err := New[saveItem]().CountOver(context.Background(), "name"); return err },
			"Cursor":    func() error { _, err := New[saveItem]().Cursor(context.Background()); return err },
			"Delete":    func() error { return New[saveItem]().Where("id", 1).Delete(context.Background()) },
			"Update":    func() error { return New[saveItem]().Update(context.Background(), &saveItem{ID: 1, Name: "x"}) },
			"UpdateColumns": func() error {
				return New[saveItem]().UpdateColumns(context.Background(), &saveItem{ID: 1, Name: "x"}, "name")
			},
			"UpdateManyByKey": func() error {
				return New[saveItem]().UpdateManyByKey(context.Background(), "id", "name", map[int]string{1: "x"})
			},
			"Exec": func() error {
				_, err := New[saveItem]().Raw("DELETE FROM save_items").Exec(context.Background())
				return err
			},
			"CreateMany": func() error { return New[saveItem]().CreateMany(context.Background(), []*saveItem{{Name: "x"}}) },
			"BulkInsert": func() error { return New[saveItem]().BulkInsert(context.Background(), []*saveItem{{Name: "x"}}) },
			"Attach": func() error {
				return New[pivotLocalParent]().Attach(context.Background(), &pivotLocalParent{ID: 1}, "Roles", []any{1}, nil)
			},
			"Detach": func() error {
				return New[pivotLocalParent]().Detach(context.Background(), &pivotLocalParent{ID: 1}, "Roles", nil)
			},
			"Load": func() error {
				return New[pivotLocalParent]().Load(context.Background(), &pivotLocalParent{ID: 1}, "Roles")
			},
			"Get":    func() error { _, err := New[saveItem]().Get(context.Background()); return err },
			"Create": func() error { return New[saveItem]().Create(context.Background(), &saveItem{Name: "x"}) },
			"UpdateMany": func() error {
				return New[saveItem]().Where("id", 1).UpdateMany(context.Background(), map[string]any{"name": "x"})
			},
			"Scalar": func() error {
				_, err := Query[int]().Table("save_items").Select("id").Get(context.Background())
				return err
			},
		} {
			t.Run(name, func(t *testing.T) {
				defer func() {
					if p := recover(); p != nil {
						t.Errorf("nil database panicked: %v", p)
					}
				}()
				if err := run(); !errors.Is(err, ErrNilDatabase) {
					t.Errorf("want ErrNilDatabase, got %v", err)
				}
			})
		}
	}
}

func TestCreateMany_HookFailureRollsBackInsertedRows(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	m := New[createAfterReject]().SetDB(db).Table("save_items")
	if err := m.CreateMany(context.Background(), []*createAfterReject{{}, {}}); !errors.Is(err, errDatabaseFailure) {
		t.Fatalf("hook failure: %v", err)
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM save_items").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("failed create hook left inserted rows: %d", count)
	}
}
