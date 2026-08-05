package zorm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// RWUser exercises the auto-transaction path: because it implements
// AfterCreateTx, Create opens a transaction of its own when called outside one.
type RWUser struct {
	ID   int64
	Name string
}

func (RWUser) TableName() string { return "rw_users" }

func (u *RWUser) AfterCreateTx(ctx context.Context, tx *Tx) error {
	_, err := tx.Tx.ExecContext(ctx,
		"INSERT INTO rw_audit (entity_id, action) VALUES (?, ?)", u.ID, "created")
	return err
}

// RWPlain has no hooks; used for the bulk/chunked paths.
type RWPlain struct {
	ID   int64
	Name string
}

func (RWPlain) TableName() string { return "rw_plain" }

// setupResolverOnly configures a primary/replica resolver as the ONLY source of
// a database handle: GlobalDB is nil, and no model calls SetDB. Write paths that
// consult the resolver work; those that reach for m.db or GlobalDB find nothing.
// The replica is an empty database, so a misrouted statement fails loudly.
func setupResolverOnly(t *testing.T) (primary *sql.DB, cleanup func()) {
	t.Helper()

	primary, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open primary: %v", err)
	}
	_, err = primary.Exec(`
		CREATE TABLE rw_users (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE rw_plain (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE rw_audit (entity_id INTEGER, action TEXT);
	`)
	if err != nil {
		t.Fatalf("failed to setup primary schema: %v", err)
	}

	replica, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open replica: %v", err)
	}

	oldDB := GlobalDB
	GlobalDB = nil
	SetGlobalDB(nil)
	ConfigureDBResolver(
		WithPrimary(primary),
		WithReplicas(replica),
	)

	return primary, func() {
		ClearDBResolver()
		SetGlobalDB(oldDB)
		GlobalDB = oldDB
		replica.Close()
		primary.Close()
	}
}

func TestAutoTx_UsesResolverPrimary(t *testing.T) {
	primary, cleanup := setupResolverOnly(t)
	defer cleanup()

	ctx := context.Background()
	user := &RWUser{Name: "Ada"}

	// Create routes through withAutoTx because RWUser implements AfterCreateTx.
	if err := New[RWUser]().Create(ctx, user); err != nil {
		t.Fatalf("Create with resolver-only setup failed: %v", err)
	}

	var n int
	if err := primary.QueryRow("SELECT COUNT(*) FROM rw_users WHERE name = 'Ada'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1 user on primary, got %d", n)
	}

	// The hook wrote through the auto-opened transaction, so its row must have
	// committed with the INSERT.
	if err := primary.QueryRow("SELECT COUNT(*) FROM rw_audit WHERE action = 'created'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1 audit row on primary, got %d", n)
	}
}

func TestBulkInsert_UsesResolverPrimary(t *testing.T) {
	primary, cleanup := setupResolverOnly(t)
	defer cleanup()

	ctx := context.Background()
	rows := []*RWPlain{{Name: "one"}, {Name: "two"}}

	if err := New[RWPlain]().BulkInsert(ctx, rows); err != nil {
		t.Fatalf("BulkInsert with resolver-only setup failed: %v", err)
	}

	var n int
	if err := primary.QueryRow("SELECT COUNT(*) FROM rw_plain").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("expected 2 rows on primary, got %d", n)
	}
}

func TestUpdateManyByKey_Chunked_UsesResolverPrimary(t *testing.T) {
	primary, cleanup := setupResolverOnly(t)
	defer cleanup()

	ctx := context.Background()

	// updateManyByKeyChunked kicks in above 500 entries.
	const entries = 501
	for i := 1; i <= entries; i++ {
		if _, err := primary.Exec("INSERT INTO rw_plain (id, name) VALUES (?, ?)", i, "before"); err != nil {
			t.Fatal(err)
		}
	}

	updates := make(map[int64]string, entries)
	for i := 1; i <= entries; i++ {
		updates[int64(i)] = "after"
	}

	if err := New[RWPlain]().UpdateManyByKey(ctx, "id", "name", updates); err != nil {
		t.Fatalf("chunked UpdateManyByKey with resolver-only setup failed: %v", err)
	}

	var n int
	if err := primary.QueryRow("SELECT COUNT(*) FROM rw_plain WHERE name = 'after'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != entries {
		t.Errorf("expected %d updated rows on primary, got %d", entries, n)
	}
}

func TestCreateMany_Chunked_UsesResolverPrimary(t *testing.T) {
	primary, cleanup := setupResolverOnly(t)
	defer cleanup()

	ctx := context.Background()

	// Chunk size is min(65535/numColumns, 5000); one more than the cap forces
	// the multi-chunk path, which opens its own transaction.
	const rows = bulkInsertMaxRowsPerStmt + 1
	entities := make([]*RWPlain, rows)
	for i := range entities {
		entities[i] = &RWPlain{Name: fmt.Sprintf("row-%d", i)}
	}

	if err := New[RWPlain]().CreateMany(ctx, entities); err != nil {
		t.Fatalf("chunked CreateMany with resolver-only setup failed: %v", err)
	}

	var n int
	if err := primary.QueryRow("SELECT COUNT(*) FROM rw_plain").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != rows {
		t.Errorf("expected %d rows on primary, got %d", rows, n)
	}
}

// The IN-clause shape is what the dialect actually drives: PostgreSQL collapses
// the list to `= ANY(?)` with one array arg, SQLite spreads it into `IN (?,?)`.
// Placeholder style is not a useful signal here because Print always rebinds.
func TestEffectiveDialect_UsesResolverPrimary(t *testing.T) {
	_, cleanup := setupResolverOnly(t)
	defer cleanup()

	// With only a resolver configured, dialect detection must still find the
	// primary's driver (SQLite) rather than falling back to PostgreSQL.
	query, args := New[RWPlain]().WhereIn("id", []any{1, 2}).Print()

	if strings.Contains(query, "ANY") {
		t.Errorf("expected a SQLite IN-list from the resolver primary, got %q", query)
	}
	if !strings.Contains(query, "IN (") {
		t.Errorf("expected a spread IN list, got %q", query)
	}
	if len(args) != 2 {
		t.Errorf("expected 2 spread args, got %d (%v)", len(args), args)
	}
}

func TestScalarEffectiveDialect_UsesResolverPrimary(t *testing.T) {
	_, cleanup := setupResolverOnly(t)
	defer cleanup()

	query, args := Query[string]().Table("rw_plain").Select("name").WhereIn("id", []any{1, 2}).Print()

	if strings.Contains(query, "ANY") {
		t.Errorf("expected a SQLite IN-list from the resolver primary, got %q", query)
	}
	if !strings.Contains(query, "IN (") {
		t.Errorf("expected a spread IN list, got %q", query)
	}
	if len(args) != 2 {
		t.Errorf("expected 2 spread args, got %d (%v)", len(args), args)
	}
}

// Transaction and Model.Transaction must open on the resolver's primary. They
// are the most public transaction entry points, so a resolver-only setup that
// works for Create must work for them too.

func TestTransaction_UsesResolverPrimary(t *testing.T) {
	primary, cleanup := setupResolverOnly(t)
	defer cleanup()

	ctx := context.Background()

	err := Transaction(ctx, func(tx *Tx) error {
		_, err := tx.Tx.ExecContext(ctx, "INSERT INTO rw_plain (name) VALUES (?)", "in-tx")
		return err
	})
	if err != nil {
		t.Fatalf("Transaction with resolver-only setup failed: %v", err)
	}

	var n int
	if err := primary.QueryRow("SELECT COUNT(*) FROM rw_plain WHERE name = 'in-tx'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1 row on primary, got %d", n)
	}
}

func TestModelTransaction_UsesResolverPrimary(t *testing.T) {
	primary, cleanup := setupResolverOnly(t)
	defer cleanup()

	ctx := context.Background()

	err := New[RWPlain]().Transaction(ctx, func(tx *Tx) error {
		return New[RWPlain]().WithTx(tx).Create(ctx, &RWPlain{Name: "model-tx"})
	})
	if err != nil {
		t.Fatalf("Model.Transaction with resolver-only setup failed: %v", err)
	}

	var n int
	if err := primary.QueryRow("SELECT COUNT(*) FROM rw_plain WHERE name = 'model-tx'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1 row on primary, got %d", n)
	}
}

func TestTransaction_RollbackOnResolverPrimary(t *testing.T) {
	primary, cleanup := setupResolverOnly(t)
	defer cleanup()

	ctx := context.Background()
	sentinel := errors.New("boom")

	err := Transaction(ctx, func(tx *Tx) error {
		if _, err := tx.Tx.ExecContext(ctx, "INSERT INTO rw_plain (name) VALUES (?)", "rolled-back"); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected the sentinel error back, got %v", err)
	}

	var n int
	if err := primary.QueryRow("SELECT COUNT(*) FROM rw_plain WHERE name = 'rolled-back'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("expected the insert rolled back on primary, found %d rows", n)
	}
}
