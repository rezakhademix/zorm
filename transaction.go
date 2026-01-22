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
	// Use GlobalDB
	if GlobalDB == nil {
		return sql.ErrConnDone
	}

	return transaction(ctx, GlobalDB, fn)
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

// WithTx sets the transaction for the model.
func (m *Model[T]) WithTx(tx *Tx) *Model[T] {
	m.tx = tx.Tx
	m.ctx = tx.ctx
	return m
}
