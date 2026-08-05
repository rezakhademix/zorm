package zorm

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// ErrRollbackFailed was declared but never wrapped into anything, so a caller
// could not tell "the operation failed and we rolled back cleanly" from "the
// operation failed and the rollback ALSO failed" — the second case leaves the
// connection in an unknown state and is exactly what a caller needs to branch on.

var errRbTest = errors.New("rollback-test sentinel")

func setupRollbackDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`CREATE TABLE rb_items (id INTEGER PRIMARY KEY, name TEXT);`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

// TestTransaction_RollbackFailureIsIdentifiable rolls the transaction back by
// hand inside the callback, so the deferred rollback fails with sql.ErrTxDone.
// Both the caller's error and the rollback failure must be identifiable.
func TestTransaction_RollbackFailureIsIdentifiable(t *testing.T) {
	db := setupRollbackDB(t)
	ctx := context.Background()

	err := New[RbItem]().SetDB(db).Transaction(ctx, func(tx *Tx) error {
		if rbErr := tx.Tx.Rollback(); rbErr != nil {
			t.Fatalf("manual rollback failed: %v", rbErr)
		}
		return errRbTest
	})

	if !errors.Is(err, errRbTest) {
		t.Errorf("expected the callback's error to remain identifiable, got %v", err)
	}
	if !errors.Is(err, ErrRollbackFailed) {
		t.Errorf("expected ErrRollbackFailed to be identifiable, got %v", err)
	}
}

// RbItem drives the auto-tx variant: its AfterCreateTx hook rolls the
// transaction back and then fails, so withAutoTx's own rollback fails too.
type RbItem struct {
	ID   int64  `zorm:"primaryKey"`
	Name string `zorm:"column:name"`
}

func (RbItem) TableName() string { return "rb_items" }

// RbHookItem is the same table with a *Tx hook attached.
type RbHookItem struct {
	ID   int64  `zorm:"primaryKey"`
	Name string `zorm:"column:name"`
}

func (RbHookItem) TableName() string { return "rb_items" }

func (r *RbHookItem) AfterCreateTx(ctx context.Context, tx *Tx) error {
	if err := tx.Tx.Rollback(); err != nil {
		return err
	}
	return errRbTest
}

// TestAutoTx_RollbackFailureIsIdentifiable covers the same wrapping in the
// executor's auto-transaction path.
func TestAutoTx_RollbackFailureIsIdentifiable(t *testing.T) {
	db := setupRollbackDB(t)
	ctx := context.Background()

	err := New[RbHookItem]().SetDB(db).Create(ctx, &RbHookItem{Name: "a"})

	if !errors.Is(err, errRbTest) {
		t.Errorf("expected the hook's error to remain identifiable, got %v", err)
	}
	if !errors.Is(err, ErrRollbackFailed) {
		t.Errorf("expected ErrRollbackFailed to be identifiable, got %v", err)
	}
}

// TestTransaction_CleanRollbackIsNotFlaggedAsFailed guards against tagging every
// rollback as failed.
func TestTransaction_CleanRollbackIsNotFlaggedAsFailed(t *testing.T) {
	db := setupRollbackDB(t)
	ctx := context.Background()

	err := New[RbItem]().SetDB(db).Transaction(ctx, func(tx *Tx) error {
		return errRbTest
	})

	if !errors.Is(err, errRbTest) {
		t.Errorf("expected the callback's error, got %v", err)
	}
	if errors.Is(err, ErrRollbackFailed) {
		t.Errorf("a clean rollback must not report ErrRollbackFailed, got %v", err)
	}
}
