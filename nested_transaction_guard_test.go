package zorm

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestModelTransaction_RejectsExistingTransaction(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	m := New[saveItem]().SetDB(db)
	err := m.Transaction(ctx, func(tx *Tx) error {
		called := false
		err := m.WithTx(tx).Transaction(ctx, func(*Tx) error { called = true; return nil })
		if !errors.Is(err, ErrInvalidModel) {
			t.Errorf("nested transaction: %v", err)
		}
		if called {
			t.Error("nested transaction callback executed")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
