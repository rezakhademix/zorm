package zorm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
)

// Tx wraps sql.Tx.
type Tx struct {
	Tx    *sql.Tx
	ctx   context.Context
	state *transactionState
}

// transactionState retains the first baseline for each entity touched by a
// managed transaction. A rollback restores it; an uncertain outcome invalidates it.
type transactionState struct {
	mu          sync.Mutex
	originals   map[uintptr]func(bool)
	callerOwned bool
}

func (s *transactionState) remember(key uintptr, restore func(bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.originals != nil {
		if _, exists := s.originals[key]; !exists {
			s.originals[key] = restore
		}
	}
}

// mergeInto transfers successful savepoint changes to the enclosing transaction.
func (s *transactionState) mergeInto(parent *transactionState) {
	if parent == nil {
		return
	}
	s.mu.Lock()
	originals := s.originals
	s.originals = nil
	s.mu.Unlock()
	for key, restore := range originals {
		parent.remember(key, restore)
	}
}

func (s *transactionState) finish(committed, rolledBack bool) {
	s.mu.Lock()
	originals := s.originals
	s.originals = nil
	s.mu.Unlock()
	if !committed {
		for _, restore := range originals {
			restore(rolledBack)
		}
	}
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
// matching where the model's writes would go. An existing transaction is
// rejected instead of opening an independent transaction.
func (m *Model[T]) Transaction(ctx context.Context, fn func(tx *Tx) error) error {
	if m.tx != nil {
		return fmt.Errorf("zorm: %w: Transaction cannot open a nested transaction; use the existing transaction", ErrInvalidModel)
	}
	return transaction(ctx, transactionDB(m.db), fn)
}

// transactionDB picks the handle to open a transaction on. Transactions carry
// writes, so the resolver's primary wins over any per-model handle — the same
// order queryerForWrite and (*Model[T]).writeDB use. preferred may be nil.
func transactionDB(preferred *sql.DB) *sql.DB {
	if resolver := GetGlobalResolver(); resolver != nil {
		return resolver.Primary()
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

	zTx := &Tx{Tx: tx, ctx: ctx, state: &transactionState{originals: make(map[uintptr]func(bool))}}

	defer func() {
		if p := recover(); p != nil {
			// Attempt rollback on panic, but still re-panic with original value
			// Rollback error is discarded as we must re-panic
			rbErr := tx.Rollback()
			zTx.state.finish(false, rbErr == nil)
			panic(p)
		} else if err != nil {
			// Wrap rollback error with original error if rollback fails.
			// Both the original error and ErrRollbackFailed stay identifiable
			// through errors.Is.
			rbErr := tx.Rollback()
			zTx.state.finish(false, rbErr == nil)
			if rbErr != nil {
				err = fmt.Errorf("%w (%w: %v)", err, ErrRollbackFailed, rbErr)
			}
		} else {
			err = tx.Commit()
			zTx.state.finish(err == nil, false)
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
	clone.txState = tx.state
	clone.ctx = tx.ctx
	return clone
}
