package zorm

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestExclude_ColumnSelection(t *testing.T) {
	tests := []struct {
		name        string
		columns     []string
		writesName  bool
		writesValue bool
	}{
		{"empty", nil, true, true},
		{"unknown", []string{"missing"}, true, true},
		{"primary key", []string{"id"}, true, true},
		{"name", []string{"name"}, false, true},
		{"duplicates", []string{"value", "value"}, true, false},
		{"all data columns", []string{"name", "value"}, false, false},
	}
	for _, operation := range []string{"Update", "Save"} {
		for _, tt := range tests {
			t.Run(operation+"/"+tt.name, func(t *testing.T) {
				db := setupSaveDB(t)
				defer db.Close()
				ctx := context.Background()
				base := New[saveItem]().SetDB(db)
				entity, err := base.Find(ctx, 1)
				if err != nil {
					t.Fatal(err)
				}
				defer ClearOriginals(entity)
				entity.Name, entity.Value = "changed", 999
				query := base.Clone()
				if query.Exclude(tt.columns...) != query {
					t.Fatal("Exclude must return the same builder")
				}
				if operation == "Update" {
					err = query.Update(ctx, entity)
				} else {
					err = query.Save(ctx, entity)
				}
				if err != nil {
					t.Fatal(err)
				}
				var name string
				var value int
				if err := db.QueryRow(`SELECT name, value FROM save_items WHERE id = 1`).Scan(&name, &value); err != nil {
					t.Fatal(err)
				}
				wantName, wantValue := "A", 10
				if tt.writesName {
					wantName = "changed"
				}
				if tt.writesValue {
					wantValue = 999
				}
				if name != wantName || value != wantValue {
					t.Errorf("stored=(%q, %d), want (%q, %d)", name, value, wantName, wantValue)
				}
				if base.IsCleanField(entity, "name") != tt.writesName || base.IsCleanField(entity, "value") != tt.writesValue {
					t.Errorf("unexpected dirty fields: %v", base.GetDirtyFields(entity))
				}
				// A fresh builder must still be able to save excluded changes.
				if err := base.Save(ctx, entity); err != nil {
					t.Fatal(err)
				}
				if err := db.QueryRow(`SELECT name, value FROM save_items WHERE id = 1`).Scan(&name, &value); err != nil || name != "changed" || value != 999 {
					t.Fatalf("pending changes lost: name=%q value=%d err=%v", name, value, err)
				}
			})
		}
	}
}

func TestExclude_TransactionCloneIsIndependent(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	ctx := context.Background()
	base := New[saveItem]().SetDB(db).Exclude("value")
	entity, err := base.Find(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(entity)
	entity.Name, entity.Value = "changed", 999
	if err := base.Transaction(ctx, func(tx *Tx) error {
		// WithTx inherits exclusions without sharing the mutable map.
		return base.WithTx(tx).Exclude("name").Save(ctx, entity)
	}); err != nil {
		t.Fatal(err)
	}
	if err := base.Save(ctx, entity); err != nil {
		t.Fatal(err)
	}
	var name string
	var value int
	if err := db.QueryRow(`SELECT name, value FROM save_items WHERE id = 1`).Scan(&name, &value); err != nil || name != "changed" || value != 10 {
		t.Fatalf("transaction clone changed base exclusions: name=%q value=%d err=%v", name, value, err)
	}
}

func TestExclude_PooledModelsDoNotLeak(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	ctx := context.Background()
	entity := &saveItem{ID: 1, Name: "changed", Value: 999}
	defer ClearOriginals(entity)
	first := Acquire[saveItem]().SetDB(db).Exclude("name")
	err := first.Update(ctx, entity)
	first.Release()
	if err != nil {
		t.Fatal(err)
	}
	second := Acquire[saveItem]().SetDB(db)
	defer second.Release()
	if err := second.Update(ctx, entity); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := db.QueryRow(`SELECT name FROM save_items WHERE id = 1`).Scan(&name); err != nil || name != "changed" {
		t.Fatalf("pooled model retained exclusions: name=%q err=%v", name, err)
	}
}

func TestExclude_UpdatedAtRemainsDirty(t *testing.T) {
	for _, operation := range []string{"Update", "Save"} {
		t.Run(operation, func(t *testing.T) {
			db := setupCreatedAtDB(t)
			ctx := context.Background()
			m := New[caItem]().SetDB(db)
			entity, err := m.Find(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer ClearOriginals(entity)
			entity.Name = "changed"
			query := m.Clone().Exclude("updated_at")
			if operation == "Update" {
				err = query.Update(ctx, entity)
			} else {
				err = query.Save(ctx, entity)
			}
			if err != nil {
				t.Fatal(err)
			}
			fresh, err := m.Find(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer ClearOriginals(fresh)
			if fresh.Name != "changed" || !fresh.UpdatedAt.Equal(caSeeded) || !m.IsDirtyField(entity, "updated_at") {
				t.Fatalf("excluded timestamp was written or marked clean: stored=%v dirty=%v", fresh, m.GetDirtyFields(entity))
			}
		})
	}
}

func TestExclude_AllColumnsStillCallsHooksWithoutSQL(t *testing.T) {
	for _, operation := range []string{"Update", "Save"} {
		t.Run(operation, func(t *testing.T) {
			db := setupSaveHookDB(t)
			ctx := context.Background()
			m := New[ShItem]().SetDB(db)
			entity, err := m.Find(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer ClearOriginals(entity)
			// Any UPDATE is a failure, even if it assigns unchanged values.
			if _, err := db.Exec(`CREATE TRIGGER reject_update BEFORE UPDATE ON sh_items BEGIN SELECT RAISE(FAIL, 'unexpected update'); END`); err != nil {
				t.Fatal(err)
			}
			entity.Name, entity.Value = "pending", 999
			query := m.Clone().Exclude("name").Exclude("value", "name")
			if operation == "Update" {
				err = query.Update(ctx, entity)
			} else {
				err = query.Save(ctx, entity)
			}
			if err != nil {
				t.Fatal(err)
			}
			if entity.BeforeCount != 1 || entity.AfterCount != 1 || len(m.GetDirtyFields(entity)) != 2 {
				t.Fatalf("no-op lost hooks or pending changes: entity=%v dirty=%v", entity, m.GetDirtyFields(entity))
			}
		})
	}
}

type excludeMappedItem struct {
	Key     string `zorm:"column:record_key;primary"`
	Name    string `zorm:"column:display_name"`
	Note    *string
	Enabled bool
	Amount  int
}

func (excludeMappedItem) TableName() string { return "exclude_mapped_items" }

func TestExclude_MappedColumnsNullableAndZeroValues(t *testing.T) {
	for _, operation := range []string{"Update", "Save"} {
		for _, column := range []string{"display_name", "Name"} {
			t.Run(operation+"/"+column, func(t *testing.T) {
				db, err := sql.Open("sqlite3", ":memory:")
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if _, err := db.Exec(`CREATE TABLE exclude_mapped_items (record_key TEXT PRIMARY KEY, display_name TEXT, note TEXT, enabled BOOLEAN, amount INTEGER);
					INSERT INTO exclude_mapped_items VALUES ('first', 'original', 'keep', true, 10)`); err != nil {
					t.Fatal(err)
				}
				ctx := context.Background()
				m := New[excludeMappedItem]().SetDB(db)
				entity, err := m.Find(ctx, "first")
				if err != nil {
					t.Fatal(err)
				}
				defer ClearOriginals(entity)
				entity.Name, entity.Note, entity.Enabled, entity.Amount = "changed", nil, false, 0
				query := m.Clone().Exclude(column, "note")
				if operation == "Update" {
					err = query.Update(ctx, entity)
				} else {
					err = query.Save(ctx, entity)
				}
				if err != nil {
					t.Fatal(err)
				}
				fresh, err := m.Find(ctx, "first")
				if err != nil {
					t.Fatal(err)
				}
				defer ClearOriginals(fresh)
				wantName := "changed"
				if column == "display_name" {
					wantName = "original"
				}
				if fresh.Name != wantName || fresh.Note == nil || *fresh.Note != "keep" || fresh.Enabled || fresh.Amount != 0 {
					t.Fatalf("incorrect partial write: %v", fresh)
				}
				if !m.IsDirtyField(entity, "note") || m.IsDirtyField(entity, "display_name") != (column == "display_name") {
					t.Fatalf("excluded fields lost their baseline: %v", m.GetDirtyFields(entity))
				}
				if err := m.Save(ctx, entity); err != nil {
					t.Fatal(err)
				}
				var name string
				var note *string
				if err := db.QueryRow(`SELECT display_name, note FROM exclude_mapped_items WHERE record_key = 'first'`).Scan(&name, &note); err != nil || name != "changed" || note != nil || m.HasDirtyFields(entity) {
					t.Fatalf("pending nullable change lost: name=%q note=%v err=%v", name, note, err)
				}
			})
		}
	}
}

func TestExclude_TransactionRollback(t *testing.T) {
	for _, operation := range []string{"Update", "Save"} {
		t.Run(operation, func(t *testing.T) {
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
			rejection := errors.New("abort transaction")
			err = m.Transaction(ctx, func(tx *Tx) error {
				query := m.WithTx(tx).Exclude("value")
				var err error
				if operation == "Update" {
					err = query.Update(ctx, entity)
				} else {
					err = query.Save(ctx, entity)
				}
				if err != nil {
					return err
				}
				return rejection
			})
			if !errors.Is(err, rejection) {
				t.Fatalf("unexpected rollback error: %v", err)
			}
			var name string
			var value int
			if err := db.QueryRow(`SELECT name, value FROM save_items WHERE id = 1`).Scan(&name, &value); err != nil || name != "A" || value != 10 {
				t.Fatalf("transaction leaked changes: name=%q value=%d err=%v", name, value, err)
			}
			if GetOriginal(entity, "value") != 10 || !m.IsDirtyField(entity, "value") {
				t.Fatal("rollback lost excluded baseline")
			}
		})
	}
}

func TestExclude_AutoTransactionFailureRollsBackHook(t *testing.T) {
	for _, operation := range []string{"Update", "Save"} {
		t.Run(operation, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			if _, err := db.Exec(`CREATE TABLE partial_audit (message TEXT);
				CREATE TRIGGER reject_update BEFORE UPDATE ON save_items BEGIN SELECT RAISE(FAIL, 'blocked'); END`); err != nil {
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
			query := m.Clone().Exclude("value")
			if operation == "Update" {
				err = query.Update(ctx, entity)
			} else {
				err = query.Save(ctx, entity)
			}
			if err == nil {
				t.Fatal("expected update failure")
			}
			var auditCount int
			if err := db.QueryRow(`SELECT COUNT(*) FROM partial_audit`).Scan(&auditCount); err != nil || auditCount != 0 {
				t.Fatalf("failed update committed hook writes: count=%d err=%v", auditCount, err)
			}
			if GetOriginal(entity, "name") != "A" || GetOriginal(entity, "value") != 10 || !m.IsDirtyField(entity, "value") {
				t.Fatalf("failed auto-transaction lost pending changes: %v", GetOriginals(entity))
			}
		})
	}
}

func TestExclude_SavePreservesConflictDetection(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		name := "stale version"
		if deleted {
			name = "deleted row"
		}
		t.Run(name, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			ctx := context.Background()
			m := New[saveVersionedItem]().SetDB(db)
			entity, err := m.Find(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer ClearOriginals(entity)
			if deleted {
				_, err = db.Exec(`DELETE FROM save_versioned_items WHERE id = 1`)
			} else {
				_, err = db.Exec(`UPDATE save_versioned_items SET name = 'winner', version = version + 1 WHERE id = 1`)
			}
			if err != nil {
				t.Fatal(err)
			}
			entity.Name, entity.Value = "changed", 999
			err = m.Clone().Exclude("value", "version").Save(ctx, entity)
			want := ErrOptimisticLock
			if deleted {
				want = ErrRecordNotFound
			}
			if !errors.Is(err, want) {
				t.Fatalf("Save error=%v, want %v", err, want)
			}
			if entity.Version != 1 || GetOriginal(entity, "name") != "A" || GetOriginal(entity, "value") != 10 || len(m.GetDirtyFields(entity)) != 2 {
				t.Fatalf("rejected Save changed tracking: entity=%v originals=%v", entity, GetOriginals(entity))
			}
		})
	}
}

func TestExclude_SaveOnlyExcludedChangePreservesTimestamp(t *testing.T) {
	db := setupCreatedAtDB(t)
	ctx := context.Background()
	m := New[caItem]().SetDB(db)
	entity, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(entity)
	entity.Name = "pending"
	if err := m.Clone().Exclude("name").Save(ctx, entity); err != nil {
		t.Fatal(err)
	}
	fresh, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(fresh)
	if fresh.Name != "orig" || !fresh.UpdatedAt.Equal(entity.UpdatedAt) || !fresh.UpdatedAt.Equal(caSeeded) {
		t.Fatalf("excluded-only Save changed stored data or the timestamp: %v", fresh)
	}
	if !m.IsDirtyField(entity, "name") || m.IsDirtyField(entity, "updated_at") {
		t.Fatalf("excluded-only Save refreshed the wrong baseline: %v", m.GetDirtyFields(entity))
	}
}
