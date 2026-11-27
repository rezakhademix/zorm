package zorm

import (
	"context"
	"errors"
	"testing"
)

func TestTrackingTransaction_UncertainOutcomeInvalidatesBaseline(t *testing.T) {
	for _, failure := range []string{"commit", "rollback"} {
		t.Run(failure, func(t *testing.T) {
			f := &failureDB{affected: 1}
			if failure == "commit" {
				f.commitErr = errDatabaseFailure
			} else {
				f.rollbackErr = errDatabaseFailure
			}
			db := openFailureDB(t, f)
			m := New[saveVersionedItem]().SetDB(db)
			entity := &saveVersionedItem{ID: 1, Name: "before", Version: 1}
			trackOriginals(entity, m.modelInfo)
			defer ClearOriginals(entity)
			entity.Name = "pending"
			ctx := context.Background()
			err := m.Transaction(ctx, func(tx *Tx) error {
				if err := m.WithTx(tx).Save(ctx, entity); err != nil {
					return err
				}
				if failure == "rollback" {
					return errDatabaseFailure
				}
				return nil
			})
			if !errors.Is(err, errDatabaseFailure) {
				t.Fatal(err)
			}
			if IsTracked(entity) {
				t.Error("uncertain transaction outcome retained a reusable baseline")
			}
		})
	}
}

func TestTrackingTransaction_PanicRestoresBaseline(t *testing.T) {
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
	func() {
		defer func() {
			if p := recover(); p != "abort" {
				t.Errorf("unexpected panic: %v", p)
			}
		}()
		_ = m.Transaction(ctx, func(tx *Tx) error {
			if err := m.WithTx(tx).Save(ctx, entity); err != nil {
				return err
			}
			panic("abort")
		})
	}()
	if GetOriginal(entity, "name") != "A" || !m.IsDirtyField(entity, "name") {
		t.Error("panic rollback lost the original baseline")
	}
}

func TestTrackingTransaction_RolledBackCreateIsUntracked(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	ctx := context.Background()
	m := New[saveItem]().SetDB(db)
	entity := &saveItem{Name: "pending"}
	defer ClearOriginals(entity)
	abort := errors.New("abort create")
	err := m.Transaction(ctx, func(tx *Tx) error {
		if err := m.WithTx(tx).Create(ctx, entity); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatal(err)
	}
	if IsTracked(entity) {
		t.Error("rolled-back insert retained its baseline")
	}
}
