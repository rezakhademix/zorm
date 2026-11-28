package zorm

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
)

func TestPartialUpdate_ExecutionPaths(t *testing.T) {
	for _, operation := range []string{"UpdateColumns", "UpdateExclude", "SaveExclude"} {
		for _, mode := range []string{"direct", "cached", "transaction", "cached transaction"} {
			for _, pending := range []int{0, 999} {
				t.Run(operation+"/"+mode+"/"+strconv.Itoa(pending), func(t *testing.T) {
					db := setupSaveDB(t)
					defer db.Close()
					ctx := context.Background()
					base := New[saveItem]().SetDB(db)
					entity, err := base.Find(ctx, 1)
					if err != nil {
						t.Fatal(err)
					}
					defer ClearOriginals(entity)
					m := base
					if mode == "cached" || mode == "cached transaction" {
						cache := NewStmtCache(10)
						defer cache.Close()
						m = m.WithStmtCache(cache)
					}
					entity.Name, entity.Value = "changed", pending
					write := func(model *Model[saveItem]) error {
						switch operation {
						case "UpdateColumns":
							return model.UpdateColumns(ctx, entity, "name")
						case "UpdateExclude":
							return model.Clone().Exclude("value").Update(ctx, entity)
						default:
							return model.Clone().Exclude("value").Save(ctx, entity)
						}
					}
					if mode == "transaction" || mode == "cached transaction" {
						err = m.Transaction(ctx, func(tx *Tx) error { return write(m.WithTx(tx)) })
					} else {
						err = write(m)
					}
					if err != nil {
						t.Fatal(err)
					}
					if original := GetOriginal(entity, "value"); original != 10 {
						t.Errorf("excluded baseline = %v, want 10", original)
					}
					if m.IsDirtyField(entity, "name") || !m.IsDirtyField(entity, "value") {
						t.Fatalf("expected only value dirty, got %v", m.GetDirtyFields(entity))
					}
					var stored int
					if err := db.QueryRow(`SELECT value FROM save_items WHERE id = 1`).Scan(&stored); err != nil || stored != 10 {
						t.Fatalf("excluded column was written: stored=%d err=%v", stored, err)
					}
					// Repeated partial writes must continue preserving the excluded baseline.
					entity.Name = "changed twice"
					if err := write(m); err != nil {
						t.Fatal(err)
					}
					if err := m.Save(ctx, entity); err != nil {
						t.Fatal(err)
					}
					if err := db.QueryRow(`SELECT value FROM save_items WHERE id = 1`).Scan(&stored); err != nil || stored != pending {
						t.Fatalf("pending value lost: stored=%d want=%d err=%v", stored, pending, err)
					}
					if m.HasDirtyFields(entity) {
						t.Error("fully persisted entity must be clean")
					}
				})
			}
		}
	}
}

func TestPartialUpdate_ColumnSelection(t *testing.T) {
	tests := []struct {
		name       string
		columns    []string
		writesName bool
		writesBoth bool
	}{
		{"empty", nil, false, false},
		{"unknown", []string{"missing"}, false, false},
		{"primary key only", []string{"id"}, false, false},
		{"mixed skipped columns", []string{"missing", "id", "name"}, false, false},
		{"duplicates", []string{"name", "name"}, true, false},
		{"all columns", []string{"name", "value"}, true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			ctx := context.Background()
			m := New[saveItem]().SetDB(db)
			entity, err := m.Find(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer ClearOriginals(entity)
			entity.Name, entity.Value = "changed", 999
			err = m.UpdateColumns(ctx, entity, tt.columns...)
			invalid := tt.name == "unknown" || tt.name == "mixed skipped columns"
			if invalid && !errors.Is(err, ErrColumnNotFound) || !invalid && err != nil {
				t.Fatal(err)
			}
			if m.IsCleanField(entity, "name") != tt.writesName || m.IsCleanField(entity, "value") != tt.writesBoth {
				t.Errorf("wrong clean columns: %v", m.GetDirtyFields(entity))
			}
			var name string
			var value int
			if err := db.QueryRow(`SELECT name, value FROM save_items WHERE id = 1`).Scan(&name, &value); err != nil {
				t.Fatal(err)
			}
			if (name == "changed") != tt.writesName || (value == 999) != tt.writesBoth {
				t.Errorf("unexpected stored values: %q %d", name, value)
			}
		})
	}
}

func TestPartialUpdate_UntrackedEntity(t *testing.T) {
	for _, operation := range []string{"UpdateColumns", "UpdateExclude", "UpdateAll", "UpdateColumnsAll"} {
		t.Run(operation, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			ctx := context.Background()
			m := New[saveItem]().SetDB(db)
			entity := &saveItem{ID: 1, Name: "changed", Value: 999}
			defer ClearOriginals(entity)
			var err error
			switch operation {
			case "UpdateColumns":
				err = m.UpdateColumns(ctx, entity, "name")
			case "UpdateExclude":
				err = m.Clone().Exclude("value").Update(ctx, entity)
			case "UpdateAll":
				err = m.Update(ctx, entity)
			case "UpdateColumnsAll":
				err = m.UpdateColumns(ctx, entity, "name", "value")
			}
			if err != nil {
				t.Fatal(err)
			}
			fullWrite := operation == "UpdateAll" || operation == "UpdateColumnsAll"
			if IsTracked(entity) != fullWrite {
				t.Errorf("tracked=%v, full write=%v", IsTracked(entity), fullWrite)
			}
			err = m.Save(ctx, entity)
			if fullWrite && err != nil || !fullWrite && !errors.Is(err, ErrSaveUntracked) {
				t.Errorf("unexpected Save result: %v", err)
			}
		})
	}
}

func TestPartialUpdate_FailurePreservesBaseline(t *testing.T) {
	for _, operation := range []string{"UpdateColumns", "UpdateExclude", "SaveExclude"} {
		for _, failure := range []string{"constraint", "canceled", "prepare"} {
			t.Run(operation+"/"+failure, func(t *testing.T) {
				db := setupSaveDB(t)
				defer db.Close()
				ctx := context.Background()
				m := New[saveItem]().SetDB(db)
				entity, err := m.Find(ctx, 1)
				if err != nil {
					t.Fatal(err)
				}
				defer ClearOriginals(entity)
				entity.Name, entity.Value = "changed", 999
				switch failure {
				case "constraint":
					if _, err := db.Exec(`CREATE TRIGGER reject_update BEFORE UPDATE ON save_items BEGIN SELECT RAISE(FAIL, 'blocked'); END`); err != nil {
						t.Fatal(err)
					}
				case "canceled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				case "prepare":
					cache := NewStmtCache(10)
					defer cache.Close()
					m = m.WithStmtCache(cache)
					if _, err := db.Exec(`DROP TABLE save_items`); err != nil {
						t.Fatal(err)
					}
				}
				switch operation {
				case "UpdateColumns":
					err = m.UpdateColumns(ctx, entity, "name")
				case "UpdateExclude":
					err = m.Exclude("value").Update(ctx, entity)
				case "SaveExclude":
					err = m.Exclude("value").Save(ctx, entity)
				}
				if err == nil {
					t.Fatal("expected write failure")
				}
				if GetOriginal(entity, "name") != "A" || GetOriginal(entity, "value") != 10 || len(m.GetDirtyFields(entity)) != 2 {
					t.Errorf("failed update changed baseline: originals=%v dirty=%v", GetOriginals(entity), m.GetDirtyFields(entity))
				}
			})
		}
	}
}

func TestPartialUpdate_TimestampsAndVersion(t *testing.T) {
	t.Run("skipped created_at and automatic updated_at", func(t *testing.T) {
		db := setupCreatedAtDB(t)
		ctx := context.Background()
		m := New[caItem]().SetDB(db)
		entity, err := m.Find(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer ClearOriginals(entity)
		entity.Name, entity.CreatedAt = "changed", time.Time{}
		if err := m.UpdateColumns(ctx, entity, "name", "created_at"); err != nil {
			t.Fatal(err)
		}
		if original := GetOriginal(entity, "created_at").(time.Time); !original.Equal(caSeeded) || !m.IsDirtyField(entity, "created_at") {
			t.Error("skipped creation timestamp was marked clean")
		}
		if entity.UpdatedAt.Equal(caSeeded) || m.IsDirtyField(entity, "updated_at") {
			t.Error("automatic update timestamp was not refreshed and marked clean")
		}
		if !storedCreatedAt(t, db).Equal(caSeeded) {
			t.Error("stored creation timestamp changed")
		}
	})
	t.Run("explicit updated_at", func(t *testing.T) {
		db := setupSaveDB(t)
		defer db.Close()
		ctx := context.Background()
		m := New[saveItemWithTouch]().SetDB(db)
		entity, err := m.Find(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer ClearOriginals(entity)
		want := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
		entity.Name, entity.UpdatedAt = "pending", want
		if err := m.UpdateColumns(ctx, entity, "updated_at"); err != nil {
			t.Fatal(err)
		}
		if !entity.UpdatedAt.Equal(want) || m.IsDirtyField(entity, "updated_at") || !m.IsDirtyField(entity, "name") {
			t.Errorf("explicit timestamp or excluded name mishandled: %v", m.GetDirtyFields(entity))
		}
	})
	t.Run("managed version and excluded data", func(t *testing.T) {
		db := setupSaveDB(t)
		defer db.Close()
		ctx := context.Background()
		m := New[saveVersionedItem]().SetDB(db)
		entity, err := m.Find(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer ClearOriginals(entity)
		entity.Name, entity.Value = "changed", 999
		if err := m.Clone().Exclude("value", "version").Save(ctx, entity); err != nil {
			t.Fatal(err)
		}
		if entity.Version != 2 || GetOriginal(entity, "version") != int64(2) || !m.IsDirtyField(entity, "value") {
			t.Errorf("version or excluded baseline incorrect: %v", GetOriginals(entity))
		}
		if err := m.Save(ctx, entity); err != nil {
			t.Fatal(err)
		}
		var value int
		var version int64
		if err := db.QueryRow(`SELECT value, version FROM save_versioned_items WHERE id = 1`).Scan(&value, &version); err != nil || value != 999 || version != 3 || m.HasDirtyFields(entity) {
			t.Fatalf("follow-up Save failed: value=%d version=%d err=%v", value, version, err)
		}
	})
}

type partialHookItem struct {
	ID     int `zorm:"primaryKey"`
	Name   string
	Value  int
	Before func(*partialHookItem) error `zorm:"-"`
	After  func(*partialHookItem) error `zorm:"-"`
}

func (partialHookItem) TableName() string { return "save_items" }

func (item *partialHookItem) BeforeUpdate(context.Context) error {
	if item.Before != nil {
		return item.Before(item)
	}
	return nil
}

func (item *partialHookItem) AfterUpdate(context.Context) error {
	if item.After != nil {
		return item.After(item)
	}
	return nil
}

func TestPartialUpdate_HookMutationsAndErrors(t *testing.T) {
	for _, operation := range []string{"UpdateColumns", "UpdateExclude", "SaveExclude"} {
		for _, stage := range []string{"before", "after"} {
			for _, rejects := range []bool{false, true} {
				name := stage + "/success"
				if rejects {
					name = stage + "/error"
				}
				t.Run(operation+"/"+name, func(t *testing.T) {
					db := setupSaveDB(t)
					defer db.Close()
					ctx := context.Background()
					m := New[partialHookItem]().SetDB(db)
					entity, err := m.Find(ctx, 1)
					if err != nil {
						t.Fatal(err)
					}
					defer ClearOriginals(entity)
					entity.Name = "changed"
					rejection := errors.New("hook rejected update")
					hook := func(item *partialHookItem) error {
						item.Name, item.Value = "hook name", 999
						if rejects {
							return rejection
						}
						return nil
					}
					if stage == "before" {
						entity.Before = hook
					} else {
						entity.After = hook
					}
					switch operation {
					case "UpdateColumns":
						err = m.UpdateColumns(ctx, entity, "name")
					case "UpdateExclude":
						err = m.Clone().Exclude("value").Update(ctx, entity)
					case "SaveExclude":
						err = m.Clone().Exclude("value").Save(ctx, entity)
					}
					if rejects && !errors.Is(err, rejection) || !rejects && err != nil {
						t.Fatalf("unexpected hook error: %v", err)
					}
					if !m.IsDirtyField(entity, "value") || GetOriginal(entity, "value") != 10 {
						t.Error("hook mutation to excluded column was marked clean")
					}
					var storedName string
					if err := db.QueryRow(`SELECT name FROM save_items WHERE id = 1`).Scan(&storedName); err != nil {
						t.Fatal(err)
					}
					wantName := "hook name"
					if stage == "after" {
						wantName = "changed"
					} else if rejects {
						wantName = "A"
					}
					if storedName != wantName || GetOriginal(entity, "name") != wantName {
						t.Errorf("persisted name and baseline differ: stored=%q original=%v want=%q", storedName, GetOriginal(entity, "name"), wantName)
					}
					entity.Before, entity.After = nil, nil
					if err := m.Save(ctx, entity); err != nil {
						t.Fatal(err)
					}
					var storedValue int
					if err := db.QueryRow(`SELECT name, value FROM save_items WHERE id = 1`).Scan(&storedName, &storedValue); err != nil || storedName != "hook name" || storedValue != 999 || m.HasDirtyFields(entity) {
						t.Fatalf("Save lost hook mutation: name=%q value=%d err=%v", storedName, storedValue, err)
					}
				})
			}
		}
	}
}

type partialTxHookItem struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (partialTxHookItem) TableName() string { return "save_items" }

func (item *partialTxHookItem) BeforeUpdateTx(ctx context.Context, tx *Tx) error {
	item.Value = 999
	_, err := tx.Tx.ExecContext(ctx, `INSERT INTO partial_audit VALUES ('before')`)
	return err
}

func TestPartialUpdate_AutoTransactionHook(t *testing.T) {
	for _, operation := range []string{"UpdateColumns", "UpdateExclude", "SaveExclude"} {
		t.Run(operation, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			if _, err := db.Exec(`CREATE TABLE partial_audit (message TEXT)`); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			m := New[partialTxHookItem]().SetDB(db)
			entity, err := m.Find(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer ClearOriginals(entity)
			entity.Name = "changed"
			switch operation {
			case "UpdateColumns":
				err = m.UpdateColumns(ctx, entity, "name")
			case "UpdateExclude":
				err = m.Clone().Exclude("value").Update(ctx, entity)
			case "SaveExclude":
				err = m.Clone().Exclude("value").Save(ctx, entity)
			}
			if err != nil {
				t.Fatal(err)
			}
			if !m.IsDirtyField(entity, "value") || GetOriginal(entity, "value") != 10 {
				t.Error("auto-transaction hook lost excluded mutation")
			}
			if err := m.Save(ctx, entity); err != nil {
				t.Fatal(err)
			}
			var stored, auditCount int
			if err := db.QueryRow(`SELECT value FROM save_items WHERE id = 1`).Scan(&stored); err != nil || stored != 999 {
				t.Fatalf("pending hook value lost: %d, err=%v", stored, err)
			}
			if err := db.QueryRow(`SELECT COUNT(*) FROM partial_audit`).Scan(&auditCount); err != nil || auditCount != 2 {
				t.Fatalf("hook writes did not commit: count=%d, err=%v", auditCount, err)
			}
		})
	}
}
