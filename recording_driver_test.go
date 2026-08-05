package zorm

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"sync"
)

// A minimal database/sql driver that records the SQL text it is handed. It is
// the only way to assert on the query a terminal method actually sends: the
// builder does not expose the generated SQL for write paths, and Print() takes
// a different code path than execution does.
//
// Used by the Exec-rebind test and by the SQL-determinism tests.

// recordedSQL collects the statements a recording connection received.
type recordedSQL struct {
	mu      sync.Mutex
	queries []string
}

func (r *recordedSQL) record(query string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queries = append(r.queries, query)
}

// all returns a copy of the recorded statements.
func (r *recordedSQL) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.queries))
	copy(out, r.queries)
	return out
}

// last returns the most recent statement, or "" if none were recorded.
func (r *recordedSQL) last() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.queries) == 0 {
		return ""
	}
	return r.queries[len(r.queries)-1]
}

// reset drops everything recorded so far.
func (r *recordedSQL) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queries = nil
}

type recordingDriver struct {
	log *recordedSQL
}

func (d *recordingDriver) Open(string) (driver.Conn, error) {
	return &recordingConn{log: d.log}, nil
}

type recordingConn struct {
	log *recordedSQL
}

func (c *recordingConn) Prepare(query string) (driver.Stmt, error) {
	c.log.record(query)
	return &recordingStmt{}, nil
}

func (c *recordingConn) Close() error { return nil }

func (c *recordingConn) Begin() (driver.Tx, error) { return recordingTx{}, nil }

type recordingTx struct{}

func (recordingTx) Commit() error   { return nil }
func (recordingTx) Rollback() error { return nil }

type recordingStmt struct{}

func (recordingStmt) Close() error { return nil }

// NumInput reports -1 so database/sql skips its own argument-count check; the
// point of this driver is to observe the SQL, not to validate binding.
func (recordingStmt) NumInput() int { return -1 }

func (recordingStmt) Exec([]driver.Value) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}

func (recordingStmt) Query([]driver.Value) (driver.Rows, error) {
	return &recordingRows{}, nil
}

// recordingRows yields a single id row, which is what the INSERT ... RETURNING
// id paths need in order to reach their normal completion.
type recordingRows struct {
	done bool
}

func (*recordingRows) Columns() []string { return []string{"id"} }
func (*recordingRows) Close() error      { return nil }

func (r *recordingRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	if len(dest) > 0 {
		dest[0] = int64(1)
	}
	return nil
}

var (
	recordingDriverOnce sync.Once
	recordingDriverLog  = &recordedSQL{}
)

// newRecordingDB returns a *sql.DB backed by the recording driver together with
// the log of statements it receives. The log is reset on each call.
func newRecordingDB() (*sql.DB, *recordedSQL) {
	recordingDriverOnce.Do(func() {
		sql.Register("zorm-recording", &recordingDriver{log: recordingDriverLog})
	})

	recordingDriverLog.reset()

	db, err := sql.Open("zorm-recording", "")
	if err != nil {
		panic(err)
	}
	return db, recordingDriverLog
}
