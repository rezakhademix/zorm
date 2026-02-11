package zorm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Tx wraps sql.Tx.
type Tx struct {
	Tx  *sql.Tx
	ctx context.Context
}

// ErrRollbackFailed is returned when transaction rollback fails
var ErrRollbackFailed = errors.New("zorm: rollback failed")

// Transaction executes a function within a transaction.
func Transaction(ctx context.Context, fn func(tx *Tx) error) error {
	// Use global database connection (thread-safe access)
	db := GetGlobalDB()
	if db == nil {
		return sql.ErrConnDone
	}

	return transaction(ctx, db, fn)
}

// Transaction executes a function within a transaction using the model's database connection.
func (m *Model[T]) Transaction(ctx context.Context, fn func(tx *Tx) error) error {
	return transaction(ctx, m.db, fn)
}

// transaction is a helper to execute a function within a transaction.
func transaction(ctx context.Context, db *sql.DB, fn func(tx *Tx) error) (err error) {
	if db == nil {
		return sql.ErrConnDone
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	zTx := &Tx{Tx: tx, ctx: ctx}

	defer func() {
		if p := recover(); p != nil {
			// Attempt rollback on panic, but still re-panic with original value
			// Rollback error is discarded as we must re-panic
			_ = tx.Rollback()
			panic(p)
		} else if err != nil {
			// Wrap rollback error with original error if rollback fails
			if rbErr := tx.Rollback(); rbErr != nil {
				err = fmt.Errorf("%w (rollback also failed: %v)", err, rbErr)
			}
		} else {
			err = tx.Commit()
		}
	}()

	err = fn(zTx)
	return err
}

// WithTx returns a clone of the model with the transaction set.
// This ensures the original model is not mutated, allowing safe reuse.
//
// Example:
//
//	base := New[User]().Where("active", true)
//	Transaction(ctx, func(tx *Tx) error {
//	    // base is not mutated; txModel is a separate copy
//	    txModel := base.WithTx(tx)
//	    return txModel.Create(ctx, &user)
//	})
//	// base can still be used outside the transaction
func (m *Model[T]) WithTx(tx *Tx) *Model[T] {
	clone := m.Clone()
	clone.tx = tx.Tx
	clone.ctx = tx.ctx
	return clone
}
