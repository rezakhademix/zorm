package zorm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"
)

// failureDB exercises database/sql's real statement, row, and transaction paths
// while allowing failures that SQLite cannot reliably produce on demand.
type failureDB struct {
	prepareErr, queryErr, execErr, beginErr, commitErr, rollbackErr error
	iterationErr, affectedErr                                       error
	columns                                                         []string
	values                                                          [][]driver.Value
	affected                                                        int64
	query                                                           func(string) (driver.Rows, error)
	onExec                                                          func(string)
	onBegin, onCommit, onRollback                                   func()
}

var errDatabaseFailure = errors.New("injected database failure")

func openFailureDB(t *testing.T, failure *failureDB) *sql.DB {
	t.Helper()
	db := sql.OpenDB(failureConnector{failure})
	t.Cleanup(func() { db.Close() })
	return db
}

type failureConnector struct{ failure *failureDB }

func (c failureConnector) Connect(context.Context) (driver.Conn, error) {
	return &failureConn{c.failure}, nil
}
func (c failureConnector) Driver() driver.Driver { return failureDriver(c) }

type failureDriver struct{ failure *failureDB }

func (d failureDriver) Open(string) (driver.Conn, error) { return &failureConn{d.failure}, nil }

type failureConn struct{ failure *failureDB }

// PostgreSQL paths bind typed slices for ANY; accept those like pgx does.
func (*failureConn) CheckNamedValue(*driver.NamedValue) error { return nil }

func (c *failureConn) Prepare(query string) (driver.Stmt, error) {
	if c.failure.prepareErr != nil {
		return nil, c.failure.prepareErr
	}
	return &failureStmt{failure: c.failure, query: query}, nil
}
func (*failureConn) Close() error { return nil }
func (c *failureConn) Begin() (driver.Tx, error) {
	if c.failure.onBegin != nil {
		c.failure.onBegin()
	}
	if c.failure.beginErr != nil {
		return nil, c.failure.beginErr
	}
	return failureTx{c.failure}, nil
}
func (c *failureConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	return c.failure.openRows(query)
}
func (c *failureConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	return c.failure.execute(query)
}

func (f *failureDB) execute(query string) (driver.Result, error) {
	if f.onExec != nil {
		f.onExec(query)
	}
	return failureResult{f}, f.execErr
}

func (f *failureDB) openRows(query string) (driver.Rows, error) {
	if f.query != nil {
		return f.query(query)
	}
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	return &failureRows{columns: f.columns, values: f.values, err: f.iterationErr}, nil
}

type failureStmt struct {
	failure *failureDB
	query   string
}

func (*failureStmt) Close() error  { return nil }
func (*failureStmt) NumInput() int { return -1 }
func (s *failureStmt) Query([]driver.Value) (driver.Rows, error) {
	return s.failure.openRows(s.query)
}
func (s *failureStmt) Exec([]driver.Value) (driver.Result, error) {
	return s.failure.execute(s.query)
}

type failureTx struct{ failure *failureDB }

func (tx failureTx) Commit() error {
	if tx.failure.onCommit != nil {
		tx.failure.onCommit()
	}
	return tx.failure.commitErr
}
func (tx failureTx) Rollback() error {
	if tx.failure.onRollback != nil {
		tx.failure.onRollback()
	}
	return tx.failure.rollbackErr
}

type failureResult struct{ failure *failureDB }

func (r failureResult) LastInsertId() (int64, error) { return 1, nil }
func (r failureResult) RowsAffected() (int64, error) {
	return r.failure.affected, r.failure.affectedErr
}

type failureRows struct {
	columns []string
	values  [][]driver.Value
	err     error
	index   int
	onClose func()
}

func (r *failureRows) Columns() []string { return r.columns }
func (r *failureRows) Close() error {
	if r.onClose != nil {
		r.onClose()
	}
	return nil
}
func (r *failureRows) Next(dest []driver.Value) error {
	if r.index < len(r.values) {
		copy(dest, r.values[r.index])
		r.index++
		return nil
	}
	if r.err != nil {
		return r.err
	}
	return io.EOF
}
