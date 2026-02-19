package zorm

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// setupJoinDB creates an in-memory SQLite DB with orders, users, and payments tables.
func setupJoinDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE join_users (
			id   INTEGER PRIMARY KEY,
			name TEXT
		);
		CREATE TABLE join_orders (
			id      INTEGER PRIMARY KEY,
			user_id INTEGER,
			amount  REAL
		);
		CREATE TABLE join_payments (
			id       INTEGER PRIMARY KEY,
			order_id INTEGER,
			status   TEXT
		);

		INSERT INTO join_users  (id, name)           VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
		INSERT INTO join_orders (id, user_id, amount) VALUES (1, 1, 100.0), (2, 1, 200.0), (3, 2, 300.0);
		INSERT INTO join_payments (id, order_id, status) VALUES (1, 1, 'paid'), (2, 2, 'pending');
	`)
	if err != nil {
		t.Fatalf("failed to setup join DB: %v", err)
	}
	return db
}

// JoinOrder is the primary model used in join tests.
type JoinOrder struct {
	ID     int
	UserID int
	Amount float64
}

func (JoinOrder) TableName() string { return "join_orders" }

func TestJoin_InnerJoin(t *testing.T) {
	db := setupJoinDB(t)
	defer db.Close()

	ctx := context.Background()

	// INNER JOIN orders with users: only orders that have a matching user
	results, err := New[JoinOrder]().
		SetDB(db).
		Join("join_users", "join_orders.user_id", "=", "join_users.id").
		Select("join_orders.id", "join_orders.user_id", "join_orders.amount").
		OrderBy("join_orders.id", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("INNER JOIN failed: %v", err)
	}
	// Charlie (user_id=3) has no orders, but all 3 orders belong to existing users
	if len(results) != 3 {
		t.Errorf("expected 3 orders from INNER JOIN, got %d", len(results))
	}
}

func TestJoin_LeftJoin_NullRows(t *testing.T) {
	db := setupJoinDB(t)
	defer db.Close()

	ctx := context.Background()

	// LEFT JOIN: query join_users as the driving table, left-join join_orders.
	// Charlie (user 3) has no orders, so join_orders.amount would be NULL.
	// Use IFNULL to coerce NULL → 0 so we can scan into float64.
	results, err := New[JoinOrder]().
		SetDB(db).
		Table("join_users").
		LeftJoin("join_orders", "join_users.id", "=", "join_orders.user_id").
		Select("join_users.id", "join_users.id AS user_id", "IFNULL(join_orders.amount, 0) AS amount").
		OrderBy("join_users.id", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("LEFT JOIN failed: %v", err)
	}
	// Alice (2 orders), Bob (1 order), Charlie (0 orders but LEFT JOIN keeps him)
	if len(results) != 4 {
		t.Errorf("expected 4 rows from LEFT JOIN (Alice×2 + Bob×1 + Charlie×1 null), got %d", len(results))
	}
}

func TestJoin_WithWhereAndOrderBy(t *testing.T) {
	db := setupJoinDB(t)
	defer db.Close()

	ctx := context.Background()

	// INNER JOIN combined with WHERE and ORDER BY
	results, err := New[JoinOrder]().
		SetDB(db).
		Join("join_users", "join_orders.user_id", "=", "join_users.id").
		Select("join_orders.id", "join_orders.user_id", "join_orders.amount").
		Where("join_orders.user_id", 1).
		OrderBy("join_orders.amount", "DESC").
		Get(ctx)

	if err != nil {
		t.Fatalf("JOIN with WHERE+ORDER BY failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results for user_id=1, got %d", len(results))
	}
	// Ordered by amount DESC: 200, 100
	if results[0].Amount != 200.0 {
		t.Errorf("expected first amount 200, got %f", results[0].Amount)
	}
}

func TestJoin_MultipleJoins(t *testing.T) {
	db := setupJoinDB(t)
	defer db.Close()

	ctx := context.Background()

	// Chain two JOINs: orders → users AND orders → payments
	results, err := New[JoinOrder]().
		SetDB(db).
		Join("join_users", "join_orders.user_id", "=", "join_users.id").
		LeftJoin("join_payments", "join_orders.id", "=", "join_payments.order_id").
		Select("join_orders.id", "join_orders.user_id", "join_orders.amount").
		OrderBy("join_orders.id", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("multiple JOINs failed: %v", err)
	}
	// orders 1,2,3 all have matching users; orders 1,2 have payments, order 3 doesn't
	if len(results) != 3 {
		t.Errorf("expected 3 results from chained JOINs, got %d", len(results))
	}
}

func TestJoin_CrossJoin(t *testing.T) {
	db := setupJoinDB(t)
	defer db.Close()

	ctx := context.Background()

	// CROSS JOIN produces a Cartesian product: 3 users × 3 orders = 9 rows
	results, err := New[JoinOrder]().
		SetDB(db).
		CrossJoin("join_users").
		Select("join_orders.id", "join_orders.user_id", "join_orders.amount").
		Get(ctx)

	if err != nil {
		t.Fatalf("CROSS JOIN failed: %v", err)
	}
	if len(results) != 9 {
		t.Errorf("expected 9 rows from CROSS JOIN (3 orders × 3 users), got %d", len(results))
	}
}

func TestJoin_InvalidTableName_SetsBuildErr(t *testing.T) {
	db := setupJoinDB(t)
	defer db.Close()

	ctx := context.Background()

	// Invalid table name with SQL injection attempt
	_, err := New[JoinOrder]().
		SetDB(db).
		Join("join_users; DROP TABLE join_orders;--", "join_orders.user_id", "=", "join_users.id").
		Get(ctx)

	if err == nil {
		t.Error("expected buildErr for invalid table name, got nil")
	}
}

func TestJoin_InvalidColumnName_SetsBuildErr(t *testing.T) {
	db := setupJoinDB(t)
	defer db.Close()

	ctx := context.Background()

	// Invalid column name
	_, err := New[JoinOrder]().
		SetDB(db).
		Join("join_users", "join_orders.user_id'--", "=", "join_users.id").
		Get(ctx)

	if err == nil {
		t.Error("expected buildErr for invalid column name, got nil")
	}
}

func TestJoin_InvalidOperator_SetsBuildErr(t *testing.T) {
	db := setupJoinDB(t)
	defer db.Close()

	ctx := context.Background()

	_, err := New[JoinOrder]().
		SetDB(db).
		Join("join_users", "join_orders.user_id", "INVALID_OP", "join_users.id").
		Get(ctx)

	if err == nil {
		t.Error("expected buildErr for invalid operator, got nil")
	}
}

func TestJoin_Clone_PreservesJoins(t *testing.T) {
	db := setupJoinDB(t)
	defer db.Close()

	ctx := context.Background()

	base := New[JoinOrder]().
		SetDB(db).
		Join("join_users", "join_orders.user_id", "=", "join_users.id").
		Select("join_orders.id", "join_orders.user_id", "join_orders.amount")

	// Clone and add WHERE condition
	q := base.Clone().Where("join_orders.user_id", 1)

	results, err := q.Get(ctx)
	if err != nil {
		t.Fatalf("cloned JOIN query failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results after clone, got %d", len(results))
	}

	// Original should still return 3
	all, err := base.Clone().Get(ctx)
	if err != nil {
		t.Fatalf("original JOIN query failed: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("expected 3 in original after clone, got %d", len(all))
	}
}

func TestJoin_Print_ContainsJoinClause(t *testing.T) {
	query, _ := New[JoinOrder]().
		Join("join_users", "join_orders.user_id", "=", "join_users.id").
		Print()

	if !strings.Contains(query, "INNER JOIN join_users ON join_orders.user_id = join_users.id") {
		t.Errorf("Print() output missing JOIN clause: %s", query)
	}
}

// J1: RightJoin() basic functionality.
// RIGHT JOIN returns all rows from the right table (join_users) and matching rows
// from the left table (join_orders). Charlie has no orders, so his row uses NULLs
// from join_orders coerced to 0 via IFNULL.
func TestJoin_RightJoin(t *testing.T) {
	db := setupJoinDB(t)
	defer db.Close()

	ctx := context.Background()

	results, err := New[JoinOrder]().
		SetDB(db).
		RightJoin("join_users", "join_orders.user_id", "=", "join_users.id").
		Select(
			"IFNULL(join_orders.id, 0) AS id",
			"join_users.id AS user_id",
			"IFNULL(join_orders.amount, 0) AS amount",
		).
		OrderBy("join_users.id", "ASC").
		Get(ctx)

	if err != nil {
		t.Fatalf("RIGHT JOIN failed: %v", err)
	}
	// Alice×2 orders + Bob×1 order + Charlie×0 orders (1 NULL row) = 4 rows
	if len(results) != 4 {
		t.Errorf("expected 4 rows from RIGHT JOIN (2+1+1), got %d", len(results))
	}
}

// J2: JOIN + Count().
// Count() builds its own query that does not include JOIN clauses.
// The WHERE condition on the primary table's column still applies, so the
// count is correct for the filtered rows even though the JOIN is dropped.
func TestJoin_WithCount(t *testing.T) {
	db := setupJoinDB(t)
	defer db.Close()

	ctx := context.Background()

	count, err := New[JoinOrder]().
		SetDB(db).
		Join("join_users", "join_orders.user_id", "=", "join_users.id").
		Where("join_orders.user_id", 1).
		Count(ctx)

	if err != nil {
		t.Fatalf("JOIN+Count failed: %v", err)
	}
	// Alice (user_id=1) has 2 orders.
	if count != 2 {
		t.Errorf("expected count 2 for user_id=1, got %d", count)
	}
}

// J3: JOIN + Limit/Offset.
// INNER JOIN with ORDER BY, LIMIT 2, OFFSET 1: skips the first order and
// returns the next two, verifying LIMIT/OFFSET are appended after JOIN clauses.
func TestJoin_WithLimitOffset(t *testing.T) {
	db := setupJoinDB(t)
	defer db.Close()

	ctx := context.Background()

	results, err := New[JoinOrder]().
		SetDB(db).
		Join("join_users", "join_orders.user_id", "=", "join_users.id").
		Select("join_orders.id", "join_orders.user_id", "join_orders.amount").
		OrderBy("join_orders.id", "ASC").
		Limit(2).
		Offset(1).
		Get(ctx)

	if err != nil {
		t.Fatalf("JOIN+Limit+Offset failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results with LIMIT 2 OFFSET 1, got %d", len(results))
	}
	// OFFSET 1 skips order id=1; the next two are id=2 and id=3.
	if results[0].ID != 2 {
		t.Errorf("expected first result ID=2 after OFFSET 1, got %d", results[0].ID)
	}
}

// J4: JOIN + Cursor().
// The Cursor code path is separate from Get(); this test confirms that JOIN
// clauses are preserved when iterating results via a cursor.
func TestJoin_WithCursor(t *testing.T) {
	db := setupJoinDB(t)
	defer db.Close()

	ctx := context.Background()

	cursor, err := New[JoinOrder]().
		SetDB(db).
		Join("join_users", "join_orders.user_id", "=", "join_users.id").
		Select("join_orders.id", "join_orders.user_id", "join_orders.amount").
		OrderBy("join_orders.id", "ASC").
		Cursor(ctx)

	if err != nil {
		t.Fatalf("JOIN+Cursor failed: %v", err)
	}
	defer cursor.Close()

	count := 0
	for cursor.Next() {
		if _, err := cursor.Scan(ctx); err != nil {
			t.Fatalf("Cursor.Scan failed: %v", err)
		}
		count++
	}
	// All 3 orders have a matching user, so INNER JOIN yields 3 rows.
	if count != 3 {
		t.Errorf("expected 3 rows via Cursor with INNER JOIN, got %d", count)
	}
}

// J5: Print() with multiple JOINs.
// Verifies that Print() emits both JOIN clauses and preserves their insertion order.
func TestJoin_Print_MultipleJoins(t *testing.T) {
	query, _ := New[JoinOrder]().
		Join("join_users", "join_orders.user_id", "=", "join_users.id").
		LeftJoin("join_payments", "join_orders.id", "=", "join_payments.order_id").
		Print()

	if !strings.Contains(query, "INNER JOIN join_users ON join_orders.user_id = join_users.id") {
		t.Errorf("Print() missing first JOIN clause: %s", query)
	}
	if !strings.Contains(query, "LEFT JOIN join_payments ON join_orders.id = join_payments.order_id") {
		t.Errorf("Print() missing second JOIN clause: %s", query)
	}
	// INNER JOIN must appear before LEFT JOIN — insertion order must be preserved.
	innerIdx := strings.Index(query, "INNER JOIN")
	leftIdx := strings.Index(query, "LEFT JOIN")
	if innerIdx == -1 || leftIdx == -1 || innerIdx > leftIdx {
		t.Errorf("JOIN clauses are out of order in Print() output: %s", query)
	}
}

