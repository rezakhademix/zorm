package zorm

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// The string branch of addWhere validated two of its three shapes: the zero-arg
// raw form ran validateWhereRawString and the no-placeholder form validated the
// leading column name, but the parameterized raw form — Where("id = ?", 1) —
// ran nothing at all, so a comment or statement separator sailed straight into
// the SQL text. ScalarQuery had the same three-way split.

func TestWhere_RawWithArgsRejectsComment(t *testing.T) {
	m := New[TestModel]().Where("id = ? --", 1)

	if m.buildErr == nil {
		t.Fatal("expected buildErr for a raw fragment containing an SQL comment")
	}
	query, _ := m.Print()
	if strings.Contains(query, "--") {
		t.Errorf("comment leaked into SQL: %q", query)
	}
}

func TestWhere_RawWithArgsRejectsStatementSeparator(t *testing.T) {
	m := New[TestModel]().Where("id = ?; DROP TABLE users", 1)

	if m.buildErr == nil {
		t.Fatal("expected buildErr for a raw fragment containing a statement separator")
	}
	query, _ := m.Print()
	if strings.Contains(query, "DROP") {
		t.Errorf("second statement leaked into SQL: %q", query)
	}
}

func TestOrWhere_RawWithArgsRejectsComment(t *testing.T) {
	m := New[TestModel]().Where("active", true).OrWhere("id = ? /* x */", 1)

	if m.buildErr == nil {
		t.Fatal("expected buildErr for a raw fragment containing a block comment")
	}
}

// TestWhere_RawWithArgsAllowsSubquery guards against overcorrecting: legitimate
// raw expressions must still pass.
func TestWhere_RawWithArgsAllowsSubquery(t *testing.T) {
	m := New[TestModel]().Where("id IN (SELECT user_id FROM memberships WHERE role = ?)", "admin")

	if m.buildErr != nil {
		t.Fatalf("valid raw fragment was rejected: %v", m.buildErr)
	}
	query, args := m.Print()
	if !strings.Contains(query, "SELECT user_id FROM memberships") {
		t.Errorf("expected the subquery in the SQL, got %q", query)
	}
	if len(args) != 1 {
		t.Errorf("expected 1 bound arg, got %v", args)
	}
}

func TestWhere_RawWithArgsErrorSurfacedByGet(t *testing.T) {
	db := setupExDB(t)
	defer db.Close()

	_, err := New[ExModel]().SetDB(db).Where("value = ? --", 10).Get(context.Background())
	if err == nil {
		t.Fatal("expected the build error to surface from Get")
	}
	if !errors.Is(err, ErrInvalidSyntax) {
		t.Errorf("expected ErrInvalidSyntax, got %v", err)
	}
}

func TestScalarQuery_RawWithArgsRejectsComment(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	q := Query[string]().SetDB(db).Table("users").Select("name").
		Where("id = ? --", 1)

	// Must be rejected as a raw fragment, not incidentally as a bad column
	// name: before the fix ScalarQuery read the whole string as a column.
	if !errors.Is(q.buildErr, ErrInvalidSyntax) {
		t.Fatalf("expected ErrInvalidSyntax, got %v", q.buildErr)
	}
}

func TestScalarQuery_RawWithArgsRejectsStatementSeparator(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	q := Query[string]().SetDB(db).Table("users").Select("name").
		Where("id = ? OR name = ?; DROP TABLE users", 1, "x")

	if !errors.Is(q.buildErr, ErrInvalidSyntax) {
		t.Fatalf("expected ErrInvalidSyntax, got %v", q.buildErr)
	}
}

// TestScalarQuery_RawWithArgsAllowsSubquery is the scalar overcorrection guard.
func TestScalarQuery_RawWithArgsAllowsSubquery(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	names, err := Query[string]().SetDB(db).Table("users").Select("name").
		Where("id IN (SELECT id FROM users WHERE id > ?)", 0).
		Get(context.Background())
	if err != nil {
		t.Fatalf("valid raw fragment failed: %v", err)
	}
	if len(names) == 0 {
		t.Error("expected rows back")
	}
}

// The N3 work documented `Where("id IN (SELECT ... WHERE x = ?)", v)` as a
// supported shape and taught ScalarQuery to accept it, but Model.where's
// arg-count switch still treated the whole fragment as a column name: the
// one-arg form appended " =" to it (invalid SQL, no error) and the two-arg form
// read the first bind value as an operator.

func TestWhere_RawFragmentWithOneArg(t *testing.T) {
	m := New[TestModel]().Where("user_age > ?", 18)

	if m.buildErr != nil {
		t.Fatalf("unexpected buildErr: %v", m.buildErr)
	}
	query, args := m.Print()
	if strings.Contains(query, "$1 =") || strings.HasSuffix(strings.TrimSpace(query), "=") {
		t.Errorf("fragment had an operator appended to it: %q", query)
	}
	if !strings.Contains(query, "user_age > $1") {
		t.Errorf("expected the fragment to survive intact, got %q", query)
	}
	if len(args) != 1 || args[0] != 18 {
		t.Errorf("expected the single bound arg, got %v", args)
	}
}

func TestWhere_RawFragmentWithTwoArgs(t *testing.T) {
	m := New[TestModel]().Where("user_age > ? OR name = ?", 18, "x")

	if m.buildErr != nil {
		t.Fatalf("unexpected buildErr: %v", m.buildErr)
	}
	query, args := m.Print()
	if !strings.Contains(query, "user_age > $1 OR name = $2") {
		t.Errorf("expected both placeholders bound in order, got %q", query)
	}
	if len(args) != 2 {
		t.Errorf("expected 2 args, got %v", args)
	}
}

// TestWhere_RawFragmentMatchesScalar pins the claim scalar.go makes in its own
// comment: both builders accept the same shapes.
func TestWhere_RawFragmentMatchesScalar(t *testing.T) {
	const frag = "id IN (SELECT id FROM memberships WHERE role = ?)"

	m := New[TestModel]().Where(frag, "admin")
	mq, ma := m.Print()

	s := Query[string]().Table("test_models").Select("id").Where(frag, "admin")
	sq, sa := s.Print()

	if m.buildErr != nil || s.buildErr != nil {
		t.Fatalf("buildErr: model=%v scalar=%v", m.buildErr, s.buildErr)
	}
	if !strings.Contains(mq, frag[:len(frag)-2]) {
		t.Errorf("model mangled the fragment: %q", mq)
	}
	if len(ma) != len(sa) {
		t.Errorf("model bound %d args, scalar bound %d", len(ma), len(sa))
	}
	if !strings.HasSuffix(mq, strings.TrimPrefix(sq, "SELECT id FROM test_models ")) {
		t.Logf("model:  %s", mq)
		t.Logf("scalar: %s", sq)
	}
}

// TestWhere_OperatorFormStillWorks guards the shape that shares the code path.
func TestWhere_OperatorFormStillWorks(t *testing.T) {
	m := New[TestModel]().Where("user_age", ">", 18)
	if m.buildErr != nil {
		t.Fatalf("unexpected buildErr: %v", m.buildErr)
	}
	query, args := m.Print()
	if !strings.Contains(query, "user_age > $1") {
		t.Errorf("operator form broke: %q", query)
	}
	if len(args) != 1 {
		t.Errorf("expected 1 arg, got %v", args)
	}
}
