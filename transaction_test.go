package zorm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
)

// Mock Driver
type mockDriver struct {
	conn *mockConn
}

func (d *mockDriver) Open(name string) (driver.Conn, error) {
	return d.conn, nil
}

type mockConn struct {
	tx  *mockTx
	err error
}

func (c *mockConn) Prepare(query string) (driver.Stmt, error) {
	return nil, nil
}

func (c *mockConn) Close() error {
	return nil
}

func (c *mockConn) Begin() (driver.Tx, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.tx, nil
}

type mockTx struct {
	committed  bool
	rolledBack bool
	err        error
}

func (t *mockTx) Commit() error {
	if t.err != nil {
		return t.err
	}
	t.committed = true
	return nil
}

func (t *mockTx) Rollback() error {
	t.rolledBack = true
	return nil
}

func init() {
	sql.Register("mock", &mockDriver{conn: &mockConn{tx: &mockTx{}}})
}

func TestTransaction_GlobalDB(t *testing.T) {
	// Setup Mock
	tx := &mockTx{}
	conn := &mockConn{tx: tx}
	sql.Register("mock_global", &mockDriver{conn: conn})

	db, err := sql.Open("mock_global", "")
	if err != nil {
		t.Fatal(err)
	}
	GlobalDB = db

	// Test Commit
	err = Transaction(context.Background(), func(tx *Tx) error {
		return nil
	})
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if !tx.committed {
		t.Error("expected commit")
	}

	// Test Rollback on Error
	tx = &mockTx{}
	conn.tx = tx
	err = Transaction(context.Background(), func(tx *Tx) error {
		return errors.New("fail")
	})
	if err == nil {
		t.Error("expected error")
	}
	if !tx.rolledBack {
		t.Error("expected rollback")
	}

	// Test Rollback on Panic
	tx = &mockTx{}
	conn.tx = tx
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic")
		}
		if !tx.rolledBack {
			t.Error("expected rollback on panic")
		}
	}()
	Transaction(context.Background(), func(tx *Tx) error {
		panic("boom")
	})
}

func TestModel_Transaction(t *testing.T) {
	// Setup Mock
	tx := &mockTx{}
	conn := &mockConn{tx: tx}
	sql.Register("mock_model", &mockDriver{conn: conn})

	db, err := sql.Open("mock_model", "")
	if err != nil {
		t.Fatal(err)
	}

	m := New[struct{}]()
	m.SetDB(db)

	// Test Commit
	err = m.Transaction(context.Background(), func(tx *Tx) error {
		return nil
	})
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if !tx.committed {
		t.Error("expected commit")
	}

	// Test Rollback
	tx = &mockTx{}
	conn.tx = tx
	err = m.Transaction(context.Background(), func(tx *Tx) error {
		return errors.New("fail")
	})
	if err == nil {
		t.Error("expected error")
	}
	if !tx.rolledBack {
		t.Error("expected rollback")
	}
}
