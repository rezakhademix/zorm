package zorm

import (
	"strings"
	"testing"
	"time"
)

// ScopeUser is a dedicated test type so we don't collide with `User` defined
// in relations_belongsto_test.go. It uses int64 IDs to keep the test fixtures
// simple — scope behavior is independent of the PK type.
type ScopeUser struct {
	ID        int64
	Name      string
	Email     string
	Role      string
	Active    bool
	CreatedAt time.Time
	LastLogin time.Time
}

func (ScopeUser) TableName() string { return "users" }

// Role filters by role column.
func Role(role string) func(*Model[ScopeUser]) *Model[ScopeUser] {
	return func(q *Model[ScopeUser]) *Model[ScopeUser] {
		return q.Where("role", role)
	}
}

// RegisteredBetween filters by created_at range (inclusive).
func RegisteredBetween(from, to time.Time) func(*Model[ScopeUser]) *Model[ScopeUser] {
	return func(q *Model[ScopeUser]) *Model[ScopeUser] {
		return q.
			Where("created_at", ">=", from).
			Where("created_at", "<=", to)
	}
}

// SearchActiveAdmins applies a search filter over active admins. The grouped
// (name OR email) condition uses the closure form of Where — there is no
// WhereGroup helper in zorm. OrderBy takes (column, direction) as two args.
func SearchActiveAdmins(search string, lastLoginAfter time.Time) func(*Model[ScopeUser]) *Model[ScopeUser] {
	return func(q *Model[ScopeUser]) *Model[ScopeUser] {
		return q.
			Where("role", "admin").
			Where("active", true).
			Where("last_login", ">", lastLoginAfter).
			Where(func(q *Model[ScopeUser]) {
				q.
					Where("name", "ILIKE", "%"+search+"%").
					OrWhere("email", "ILIKE", "%"+search+"%")
			}).
			OrderBy("last_login", "DESC")
	}
}

func TestScope_Role(t *testing.T) {
	sql, args := New[ScopeUser]().Scope(Role("admin")).Print()

	if !strings.Contains(sql, "role = $1") {
		t.Errorf("expected SQL to contain 'role = $1', got %q", sql)
	}
	if len(args) != 1 || args[0] != "admin" {
		t.Errorf("expected args [admin], got %v", args)
	}
}

func TestScope_RegisteredBetween(t *testing.T) {
	from := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC)

	sql, args := New[ScopeUser]().Scope(RegisteredBetween(from, to)).Print()

	if !strings.Contains(sql, "created_at >= $1") {
		t.Errorf("expected SQL to contain 'created_at >= $1', got %q", sql)
	}
	if !strings.Contains(sql, "created_at <= $2") {
		t.Errorf("expected SQL to contain 'created_at <= $2', got %q", sql)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d (%v)", len(args), args)
	}
	if args[0] != from || args[1] != to {
		t.Errorf("expected args [%v %v], got %v", from, to, args)
	}
}

// TestScope_SearchActiveAdmins verifies a scope combining top-level Where
// calls with a nested Where(func(q){...}).OrWhere(...) group:
//
//  1. OrWhere(col, op, val) — the three-arg form — mirrors Where's three-arg
//     form, so OrWhere("email", "ILIKE", "%a%") emits `email ILIKE ?` with a
//     single bind arg.
//
//  2. The Where(func(q){...}) grouping helper preserves the AND/OR connectors
//     between nested predicates: `(name ILIKE $4 OR email ILIKE $5)`.
func TestScope_SearchActiveAdmins(t *testing.T) {
	since := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	sql, args := New[ScopeUser]().Scope(SearchActiveAdmins("alice", since)).Print()

	// Top-level ANDed conditions.
	for _, want := range []string{
		"role = $1",
		"active = $2",
		"last_login > $3",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("expected SQL to contain %q, got %q", want, sql)
		}
	}

	// The grouped OR search keeps both its connector and its operator.
	const wantGroup = "(name ILIKE $4 OR email ILIKE $5)"
	if !strings.Contains(sql, wantGroup) {
		t.Errorf("expected group %q in SQL, got %q", wantGroup, sql)
	}

	if !strings.Contains(sql, "ORDER BY last_login DESC") {
		t.Errorf("expected 'ORDER BY last_login DESC', got %q", sql)
	}

	wantArgs := []any{"admin", true, since, "%alice%", "%alice%"}
	if len(args) != len(wantArgs) {
		t.Fatalf("expected %d args, got %d (%v)", len(wantArgs), len(args), args)
	}
	for i, w := range wantArgs {
		if args[i] != w {
			t.Errorf("arg[%d]: expected %v, got %v", i, w, args[i])
		}
	}
}

// TestScope_ChainedWithOtherBuilders verifies the parameterised scopes compose
// with regular builder methods on either side of the Scope call.
func TestScope_ChainedWithOtherBuilders(t *testing.T) {
	from := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC)

	sql, args := New[ScopeUser]().
		Where("deleted_at IS NULL").
		Scope(Role("admin")).
		Scope(RegisteredBetween(from, to)).
		Limit(10).
		Print()

	for _, want := range []string{
		"deleted_at IS NULL",
		"role = ",
		"created_at >= ",
		"created_at <= ",
		"LIMIT 10",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("expected SQL to contain %q, got %q", want, sql)
		}
	}
	// admin, from, to — the IS NULL predicate is inlined and adds no arg.
	if len(args) != 3 {
		t.Fatalf("expected 3 args, got %d (%v)", len(args), args)
	}
}
