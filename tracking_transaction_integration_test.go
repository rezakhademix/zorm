package zorm

import (
	"context"
	"errors"
	"testing"
)

func TestTrackingTransaction_RollbackPreservesPendingWrites(t *testing.T) {
	for _, operation := range []string{"Update", "UpdateColumns", "Save"} {
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
			entity.Name = "pending"
			abort := errors.New("abort write")
			err = m.Transaction(ctx, func(tx *Tx) error {
				q := m.WithTx(tx)
				switch operation {
				case "Update":
					err = q.Update(ctx, entity)
				case "UpdateColumns":
					err = q.UpdateColumns(ctx, entity, "name")
				case "Save":
					err = q.Save(ctx, entity)
				}
				if err != nil {
					return err
				}
				return abort
			})
			if !errors.Is(err, abort) {
				t.Fatal(err)
			}
			if GetOriginal(entity, "name") != "A" || !m.IsDirtyField(entity, "name") {
				t.Error("rollback marked the pending write clean")
			}
			if err := m.Save(ctx, entity); err != nil {
				t.Fatal(err)
			}
			var name string
			if err := db.QueryRow("SELECT name FROM save_items WHERE id = 1").Scan(&name); err != nil {
				t.Fatal(err)
			}
			if name != "pending" {
				t.Errorf("retry lost pending write: stored %q", name)
			}
		})
	}
}

func TestTrackingTransaction_RepeatedSaveRestoresInitialVersion(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	ctx := context.Background()
	m := New[saveVersionedItem]().SetDB(db)
	entity, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(entity)
	abort := errors.New("abort repeated saves")
	err = m.Transaction(ctx, func(tx *Tx) error {
		q := m.WithTx(tx)
		for _, name := range []string{"first", "pending"} {
			entity.Name = name
			if err := q.Save(ctx, entity); err != nil {
				return err
			}
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatal(err)
	}
	if entity.Version != 1 || GetOriginal(entity, "version") != int64(1) {
		t.Errorf("rollback retained an uncommitted version: entity=%d, original=%v", entity.Version, GetOriginal(entity, "version"))
	}
	if err := m.Save(ctx, entity); err != nil {
		t.Fatal(err)
	}
	var name string
	var version int64
	if err := db.QueryRow("SELECT name, version FROM save_versioned_items WHERE id = 1").Scan(&name, &version); err != nil {
		t.Fatal(err)
	}
	if name != "pending" || version != 2 {
		t.Errorf("retry lost pending write: name=%q, version=%d", name, version)
	}
}

type trackingTxHookItem struct {
	ID      int
	Name    string
	Value   int
	Version int64 `zorm:"version"`
	Fail    bool  `zorm:"-"`
}

func (trackingTxHookItem) TableName() string { return "save_versioned_items" }

func (e *trackingTxHookItem) AfterUpdateTx(context.Context, *Tx) error {
	if e.Fail {
		return errors.New("reject update")
	}
	return nil
}

func TestTrackingTransaction_AutoRollbackRestoresBaseline(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	ctx := context.Background()
	m := New[trackingTxHookItem]().SetDB(db)
	entity, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(entity)
	entity.Name, entity.Fail = "pending", true
	if err := m.Save(ctx, entity); err == nil {
		t.Fatal("expected AfterUpdateTx failure")
	}
	if entity.Version != 1 || !m.IsDirtyField(entity, "name") {
		t.Error("auto-rollback retained an uncommitted baseline or version")
	}
	entity.Fail = false
	if err := m.Save(ctx, entity); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := db.QueryRow("SELECT name FROM save_versioned_items WHERE id = 1").Scan(&name); err != nil || name != "pending" {
		t.Fatalf("retry did not persist pending write: %q, %v", name, err)
	}
}

func TestTrackingTransaction_UnmanagedWriteRequiresOriginalTransaction(t *testing.T) {
	for _, outcome := range []string{"commit", "rollback"} {
		t.Run(outcome, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			ctx := context.Background()
			m := New[saveItem]().SetDB(db)
			entity, err := m.Find(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer ClearOriginals(entity)
			entity.Name = "pending"
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			bound := m.WithTx(&Tx{Tx: tx, ctx: ctx})
			if err := bound.Save(ctx, entity); err != nil {
				t.Fatal(err)
			}
			entity.Name = "second pending write"
			if err := bound.Save(ctx, entity); err != nil {
				t.Fatal(err)
			}
			if outcome == "commit" {
				err = tx.Commit()
			} else {
				err = tx.Rollback()
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := m.Save(ctx, entity); !errors.Is(err, ErrSaveUntracked) {
				t.Fatalf("unmanaged transaction left an unbound reusable baseline: %v", err)
			}
			fresh, err := m.Find(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer ClearOriginals(fresh)
			want := "A"
			if outcome == "commit" {
				want = "second pending write"
			}
			if fresh.Name != want {
				t.Errorf("stored name=%q, want %q", fresh.Name, want)
			}
		})
	}
}

type trackingTxParent struct {
	ID    int
	Name  string
	Value int
	Child *saveVersionedItem
}

func (trackingTxParent) TableName() string { return "save_items" }

func (e *trackingTxParent) AfterUpdateTx(ctx context.Context, tx *Tx) error {
	if err := New[saveVersionedItem]().WithTx(tx).Save(ctx, e.Child); err != nil {
		return err
	}
	return errors.New("reject parent and child")
}

func TestTrackingTransaction_HookSharesRollbackMetadata(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	ctx := context.Background()
	m := New[trackingTxParent]().SetDB(db)
	parent, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(parent)
	child, err := New[saveVersionedItem]().SetDB(db).Find(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(child)
	parent.Name, child.Name = "parent pending", "child pending"
	parent.Child = child
	if err := m.Save(ctx, parent); err == nil {
		t.Fatal("expected hook to roll back both writes")
	}
	if GetOriginal(parent, "name") != "A" || GetOriginal(child, "name") != "A" || child.Version != 1 {
		t.Error("hook's transaction wrapper lost rollback metadata")
	}
	if !m.IsDirtyField(parent, "name") || !New[saveVersionedItem]().IsDirtyField(child, "name") {
		t.Error("hook rollback lost pending parent or child edits")
	}
}

func TestTrackingTransaction_PartialWriteCannotReuseForeignBaseline(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	ctx := context.Background()
	m := New[saveItem]().SetDB(db)
	entity, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(entity)
	entity.Name = "rolled back"
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := m.WithTx(&Tx{Tx: tx, ctx: ctx}).Save(ctx, entity); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	entity.Value = 30
	if err := m.UpdateColumns(ctx, entity, "value"); err != nil {
		t.Fatal(err)
	}
	if err := m.Save(ctx, entity); !errors.Is(err, ErrSaveUntracked) {
		t.Fatalf("partial write made a foreign transaction's baseline reusable: %v", err)
	}
	var name string
	var value int
	if err := db.QueryRow("SELECT name, value FROM save_items WHERE id = 1").Scan(&name, &value); err != nil {
		t.Fatal(err)
	}
	if name != "A" || value != 30 {
		t.Errorf("partial write changed unwritten data: name=%q, value=%d", name, value)
	}
}
