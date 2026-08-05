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

// ErrRollbackFailed is joined into the returned error when a transaction fails
// and the subsequent rollback fails too. Test for it with errors.Is: the
// original error stays identifiable alongside it. It matters because a failed
// rollback leaves the connection in an unknown state, which callers may want to
// handle differently from an ordinary failed-and-rolled-back transaction.
var ErrRollbackFailed = errors.New("zorm: rollback failed")

// Transaction executes a function within a transaction.
// When a DBResolver is configured, the transaction is opened on the primary;
// otherwise the global database connection is used.
func Transaction(ctx context.Context, fn func(tx *Tx) error) error {
	db := transactionDB(nil)
	if db == nil {
		return sql.ErrConnDone
	}

	return transaction(ctx, db, fn)
}

// Transaction executes a function within a transaction using the model's database connection.
// When a DBResolver is configured, the transaction is opened on the primary,
// matching where the model's writes would go.
func (m *Model[T]) Transaction(ctx context.Context, fn func(tx *Tx) error) error {
	return transaction(ctx, transactionDB(m.db), fn)
}

// transactionDB picks the handle to open a transaction on. Transactions carry
// writes, so the resolver's primary wins over any per-model handle — the same
// order queryerForWrite and (*Model[T]).writeDB use. preferred may be nil.
func transactionDB(preferred *sql.DB) *sql.DB {
	if resolver := GetGlobalResolver(); resolver != nil {
		if db := resolver.Primary(); db != nil {
			return db
		}
	}
	if preferred != nil {
		return preferred
	}
	return GetGlobalDB()
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
			// Wrap rollback error with original error if rollback fails.
			// Both the original error and ErrRollbackFailed stay identifiable
			// through errors.Is.
			if rbErr := tx.Rollback(); rbErr != nil {
				err = fmt.Errorf("%w (%w: %v)", err, ErrRollbackFailed, rbErr)
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
