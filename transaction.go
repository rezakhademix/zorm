package zorm

import (
	"context"
	"database/sql"
)

// Tx wraps sql.Tx.
type Tx struct {
	Tx  *sql.Tx
	ctx context.Context
}

// Transaction executes a function within a transaction.
func Transaction(ctx context.Context, fn func(tx *Tx) error) error {
	// Use GlobalDB
	if GlobalDB == nil {
		return sql.ErrConnDone
	}

	tx, err := GlobalDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	zTx := &Tx{Tx: tx, ctx: ctx}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()

	if err := fn(zTx); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

// WithTx sets the transaction for the model.
func (m *Model[T]) WithTx(tx *Tx) *Model[T] {
	m.tx = tx.Tx
	m.ctx = tx.ctx
	return m
}

// We also need to update executor to use m.tx if present.
// Currently executor uses m.db.QueryContext.
// We should change it to use a helper `m.queryer()` which returns `m.tx` or `m.db`.
