package zorm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// CoEvent has a BLOB partition column, which database/sql scans into []byte —
// an unhashable type that used to panic when used as a map key.
type CoEvent struct {
	ID     int    `zorm:"primaryKey"`
	Bucket []byte `zorm:"column:bucket"`
	Kind   string `zorm:"column:kind"`
}

func (CoEvent) TableName() string { return "co_events" }

func setupCountOverDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`
		CREATE TABLE co_events (id INTEGER PRIMARY KEY, bucket BLOB, kind TEXT);
		INSERT INTO co_events (bucket, kind) VALUES
			(X'0A0B', 'click'), (X'0A0B', 'click'), (X'0C0D', 'view');
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

// TestCountOver_ByteSliceKeyDoesNotPanic covers partitioning on a column that
// scans as []byte. Storing that value straight into a map[any]int64 panicked
// with "hash of unhashable type []uint8".
func TestCountOver_ByteSliceKeyDoesNotPanic(t *testing.T) {
	db := setupCountOverDB(t)
	ctx := context.Background()

	counts, err := New[CoEvent]().SetDB(db).CountOver(ctx, "bucket")
	if err != nil {
		t.Fatalf("CountOver failed: %v", err)
	}

	if len(counts) != 2 {
		t.Fatalf("expected 2 partitions, got %d (%v)", len(counts), counts)
	}
	if got := counts["\x0a\x0b"]; got != 2 {
		t.Errorf("expected 2 rows in the X'0A0B' partition, got %d (%v)", got, counts)
	}
	if got := counts["\x0c\x0d"]; got != 1 {
		t.Errorf("expected 1 row in the X'0C0D' partition, got %d (%v)", got, counts)
	}
}

// TestCountOver_KeysAreNormalizedStrings pins the key normalization: every key
// is the anyToKeyString form of the column value, regardless of the type the
// driver produced, matching how the relation loaders key their maps.
//
// The return type is map[string]int64 rather than map[any]int64 on purpose:
// with `any` keys, a caller written against the old driver-typed keys
// (counts[int64(5)]) still compiles and silently reads zero. Making the key
// type explicit turns that into a compile error.
func TestCountOver_KeysAreNormalizedStrings(t *testing.T) {
	db := setupCountOverDB(t)
	ctx := context.Background()

	counts, err := New[CoEvent]().SetDB(db).CountOver(ctx, "id")
	if err != nil {
		t.Fatalf("CountOver failed: %v", err)
	}
	if len(counts) != 3 {
		t.Fatalf("expected 3 partitions, got %d (%v)", len(counts), counts)
	}
	if counts["1"] != 1 {
		t.Errorf("expected partition %q to hold 1 row, got %v", "1", counts)
	}

	// The declared type must be map[string]int64, not map[any]int64.
	var typed map[string]int64 = counts
	_ = typed
}

// TestCountOver_WithCTE verifies CountOver emits the WITH clause and binds its
// arguments. It hand-rolls its own SELECT, so it used to drop both — producing
// a query referencing an undefined CTE with mismatched placeholders.
func TestCountOver_WithCTE(t *testing.T) {
	db := setupCountOverDB(t)
	ctx := context.Background()

	sub := New[CoEvent]().SetDB(db).Select("id").Where("kind", "click")

	counts, err := New[CoEvent]().SetDB(db).
		WithCTE("clicks", sub).
		Where("id IN (SELECT id FROM clicks)").
		CountOver(ctx, "kind")
	if err != nil {
		t.Fatalf("CountOver with CTE failed: %v", err)
	}
	if len(counts) != 1 {
		t.Fatalf("expected 1 partition, got %d (%v)", len(counts), counts)
	}
	if counts["click"] != 2 {
		t.Errorf("expected 2 clicks, got %v", counts)
	}
}

// TestCountOver_UsesStmtCache verifies CountOver goes through the prepared
// statement cache when one is configured, like the other aggregates do.
func TestCountOver_UsesStmtCache(t *testing.T) {
	db := setupCountOverDB(t)
	ctx := context.Background()

	cache := NewStmtCache(10)
	defer cache.Close()

	m := New[CoEvent]().SetDB(db).WithStmtCache(cache)
	if _, err := m.CountOver(ctx, "kind"); err != nil {
		t.Fatalf("CountOver failed: %v", err)
	}
	if cache.Len() == 0 {
		t.Error("expected CountOver to populate the statement cache")
	}
}

func TestCountOver_RejectsGroupedQueriesBeforeSQL(t *testing.T) {
	ctx := context.Background()
	for _, shape := range []string{"group", "having", "both"} {
		for _, mode := range []string{"direct", "cached", "transaction"} {
			t.Run(shape+"/"+mode, func(t *testing.T) {
				queries := 0
				db := openFailureDB(t, &failureDB{query: func(string) (driver.Rows, error) {
					queries++
					return nil, errDatabaseFailure
				}})
				m := New[CoEvent]().SetDB(db).Where("kind", "click")
				if shape != "having" {
					m.GroupBy("bucket")
				}
				if shape != "group" {
					m.Having("COUNT(*) > ?", 1000)
				}
				cache := NewStmtCache(10)
				defer cache.Close()
				if mode == "cached" {
					m.WithStmtCache(cache)
				}
				run := func(q *Model[CoEvent]) error {
					before, beforeArgs := q.Print()
					counts, err := q.CountOver(ctx, "bucket")
					if !errors.Is(err, ErrGroupedAggregateRequiresGet) || counts != nil {
						t.Errorf("expected grouped aggregate rejection, got counts=%v, err=%v", counts, err)
					}
					after, afterArgs := q.Print()
					if before != after || !reflect.DeepEqual(beforeArgs, afterArgs) {
						t.Error("rejected CountOver changed query state")
					}
					return nil
				}
				if mode == "transaction" {
					if err := m.Transaction(ctx, func(tx *Tx) error { return run(m.WithTx(tx)) }); err != nil {
						t.Fatal(err)
					}
				} else {
					_ = run(m)
				}
				if queries != 0 || cache.Len() != 0 {
					t.Errorf("rejected CountOver reached SQL: queries=%d cache=%d", queries, cache.Len())
				}
			})
		}
	}
}
