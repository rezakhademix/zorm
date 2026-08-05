package zorm

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// Where's arg-count switch ended in `default: return m`, so a call with more
// args than any supported form built nothing at all and the query ran without
// the filter the caller wrote.

func TestWhere_TooManyArgsSetsBuildErr(t *testing.T) {
	m := New[TestModel]().Where("age", ">", 18, 99)

	if m.buildErr == nil {
		t.Fatal("expected buildErr for a Where call with 4 arguments")
	}

	query, args := m.Print()
	if strings.Contains(query, "age") {
		t.Errorf("rejected predicate leaked into SQL: %q", query)
	}
	if len(args) != 0 {
		t.Errorf("expected no bound args, got %v", args)
	}
}

func TestOrWhere_TooManyArgsSetsBuildErr(t *testing.T) {
	m := New[TestModel]().Where("name", "John").OrWhere("age", ">", 18, 99)

	if m.buildErr == nil {
		t.Fatal("expected buildErr for an OrWhere call with 4 arguments")
	}
}

func TestWhere_TooManyArgsSurfacedByGet(t *testing.T) {
	db := setupExDB(t)
	defer db.Close()

	_, err := New[ExModel]().SetDB(db).Where("value", ">", 1, 2).Get(context.Background())
	if err == nil {
		t.Fatal("expected the build error to surface from Get")
	}
	if errors.Is(err, ErrRecordNotFound) {
		t.Errorf("expected a build error, got %v", err)
	}
}

// TestWhere_SupportedArgCountsUnaffected guards the forms that must keep working.
func TestWhere_SupportedArgCountsUnaffected(t *testing.T) {
	m := New[TestModel]().Where("name", "John").Where("age", ">", 18)
	if m.buildErr != nil {
		t.Fatalf("valid Where forms set buildErr: %v", m.buildErr)
	}

	query, args := m.Print()
	if !strings.Contains(query, "name =") || !strings.Contains(query, "age >") {
		t.Errorf("unexpected SQL: %q", query)
	}
	if len(args) != 2 {
		t.Errorf("expected 2 args, got %v", args)
	}
}
