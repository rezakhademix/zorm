package zorm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

var errCursorIteration = errors.New("cursor driver iteration failed")
var cursorErrorDriverOnce sync.Once

type cursorErrorDriver struct{}

func (cursorErrorDriver) Open(mode string) (driver.Conn, error) {
	return &cursorErrorConn{mode: mode}, nil
}

type cursorErrorConn struct{ mode string }

func (*cursorErrorConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare unsupported")
}
func (*cursorErrorConn) Close() error              { return nil }
func (*cursorErrorConn) Begin() (driver.Tx, error) { return nil, errors.New("transaction unsupported") }
func (c *cursorErrorConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &cursorErrorRows{mode: c.mode}, nil
}

type cursorErrorRows struct {
	mode  string
	index int
}

func (*cursorErrorRows) Columns() []string { return []string{"id", "value", "name"} }
func (*cursorErrorRows) Close() error      { return nil }
func (r *cursorErrorRows) Next(dest []driver.Value) error {
	if r.mode == "before" || (r.mode == "after" && r.index == 1) {
		return errCursorIteration
	}
	if r.mode == "deadline" && r.index == 1 {
		return context.DeadlineExceeded
	}
	if r.index == 2 || r.mode == "empty" {
		return io.EOF
	}
	r.index++
	dest[0], dest[1], dest[2] = int64(r.index), int64(10), "row"
	if r.mode == "scan-error" {
		dest[0] = "invalid-integer"
	}
	return nil
}

func openCursorErrorDB(t *testing.T, mode string) *sql.DB {
	t.Helper()
	cursorErrorDriverOnce.Do(func() { sql.Register("zorm-cursor-error", cursorErrorDriver{}) })
	db, err := sql.Open("zorm-cursor-error", mode)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// Use an interface so the regression compiles before Cursor.Err is added.
func cursorErr(t *testing.T, cursor *Cursor[ExModel]) error {
	t.Helper()
	reader, ok := any(cursor).(interface{ Err() error })
	if !ok {
		t.Fatal("Cursor must expose Err() so iteration failures are observable")
	}
	return reader.Err()
}

func TestCursorErr_DriverFailure(t *testing.T) {
	for _, mode := range []string{"before", "after"} {
		t.Run(mode, func(t *testing.T) {
			cursor, err := New[ExModel]().SetDB(openCursorErrorDB(t, mode)).Cursor(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer cursor.Close()
			count := 0
			for cursor.Next() {
				if _, err := cursor.Scan(context.Background()); err != nil {
					t.Fatal(err)
				}
				count++
			}
			if mode == "after" && count != 1 {
				t.Fatalf("scanned %d rows, want 1", count)
			}
			if err := cursorErr(t, cursor); !errors.Is(err, errCursorIteration) {
				t.Fatalf("Err = %v, want driver error", err)
			}
			if err := cursor.Close(); err != nil {
				t.Fatal(err)
			}
			if err := cursor.Err(); !errors.Is(err, errCursorIteration) {
				t.Fatalf("error lost after Close: %v", err)
			}
		})
	}
}

func TestCursorErr_ExhaustionAndDeadline(t *testing.T) {
	for _, tc := range []struct {
		mode  string
		count int
		err   error
	}{
		{"normal", 2, nil},
		{"empty", 0, nil},
		{"deadline", 1, context.DeadlineExceeded},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			cursor, err := New[ExModel]().SetDB(openCursorErrorDB(t, tc.mode)).Cursor(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer cursor.Close()
			if err := cursor.Err(); err != nil {
				t.Fatalf("Err before iteration = %v", err)
			}
			count := 0
			for cursor.Next() {
				row, err := cursor.Scan(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				count++
				if row.ID != count {
					t.Fatalf("row ID = %d, want %d", row.ID, count)
				}
			}
			if count != tc.count {
				t.Fatalf("scanned %d rows, want %d", count, tc.count)
			}
			if err := cursor.Err(); !errors.Is(err, tc.err) {
				t.Fatalf("Err = %v, want %v", err, tc.err)
			}
			if cursor.Next() {
				t.Fatal("iteration restarted after exhaustion")
			}
			if err := cursor.Close(); err != nil {
				t.Fatal(err)
			}
			if err := cursor.Err(); !errors.Is(err, tc.err) {
				t.Fatalf("Err after Close = %v, want %v", err, tc.err)
			}
		})
	}
}

func TestCursorErr_EarlyClose(t *testing.T) {
	cursor, err := New[ExModel]().SetDB(openCursorErrorDB(t, "normal")).Cursor(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := cursor.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if cursor.Next() {
		t.Fatal("Next returned true after Close")
	}
	if err := cursor.Err(); err != nil {
		t.Fatalf("early Close fabricated iteration error: %v", err)
	}
}

func TestCursorErr_ScanErrorIsReturnedByScan(t *testing.T) {
	cursor, err := New[ExModel]().SetDB(openCursorErrorDB(t, "scan-error")).Cursor(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if !cursor.Next() {
		t.Fatal("expected first row")
	}
	if _, err := cursor.Scan(context.Background()); err == nil {
		t.Fatal("expected conversion error from Scan")
	}
	// Match sql.Rows: scan conversion errors are separate from iteration errors.
	if err := cursor.Err(); err != nil {
		t.Fatalf("Err should report iteration errors only, got %v", err)
	}
}

func TestCursorErr_QueryContextFailures(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "canceled"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			db := setupExDB(t)
			defer db.Close()
			ctx, cancel := context.WithCancel(context.Background())
			want := context.Canceled
			if deadline {
				cancel()
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				want = context.DeadlineExceeded
			}
			defer cancel()
			cancel()
			cursor, err := New[ExModel]().SetDB(db).Cursor(ctx)
			if cursor != nil || !errors.Is(err, want) {
				t.Fatalf("Cursor = %v, %v, want nil, %v", cursor, err, want)
			}
		})
	}
}

func TestCursorErr_ContextDeadlineDuringIteration(t *testing.T) {
	db := setupExDB(t)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cursor, err := New[ExModel]().SetDB(db).Cursor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if !cursor.Next() {
		t.Fatalf("expected first row: %v", cursor.Err())
	}
	if _, err := cursor.Scan(ctx); err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	if cursor.Next() {
		t.Fatal("expected expired iteration to stop")
	}
	if err := cursor.Err(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Err = %v, want DeadlineExceeded", err)
	}
}

func TestCursorErr_Cancellation(t *testing.T) {
	for _, afterRow := range []bool{false, true} {
		name := "before-first-row"
		if afterRow {
			name = "after-first-row"
		}
		t.Run(name, func(t *testing.T) {
			db := setupExDB(t)
			defer db.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cursor, err := New[ExModel]().SetDB(db).Cursor(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer cursor.Close()
			if afterRow {
				if !cursor.Next() {
					t.Fatal("expected first row")
				}
				if _, err := cursor.Scan(ctx); err != nil {
					t.Fatal(err)
				}
			}
			cancel()
			if cursor.Next() {
				t.Fatal("expected canceled iteration to stop")
			}
			if err := cursorErr(t, cursor); !errors.Is(err, context.Canceled) {
				t.Fatalf("Err = %v, want context.Canceled", err)
			}
		})
	}
}
