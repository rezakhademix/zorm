package zorm

import (
	"fmt"
	"strings"
	"testing"
)

func TestPrint_SimpleSelect(t *testing.T) {
	m := New[TestModel]()
	m.Where("status", "active").Limit(10)

	sql, args := m.Print()
	expected := "SELECT * FROM test_models WHERE 1=1  AND (status = ?) LIMIT 10"

	if strings.TrimSpace(sql) != expected {
		t.Errorf("expected sql %q, got %q", expected, sql)
	}
	if len(args) != 1 || args[0] != "active" {
		t.Errorf("expected args ['active'], got %v", args)
	}
}

func TestPrint_ComplexQuery(t *testing.T) {
	m := New[TestModel]()
	m.Select("id", "name").
		Where("age", 25).
		Where("status", "active").
		OrderBy("created_at", "DESC").
		Limit(5).
		Offset(10)

	sql, args := m.Print()
	expected := "SELECT id, name FROM test_models WHERE 1=1  AND (age = ?) AND (status = ?) ORDER BY created_at DESC LIMIT 5 OFFSET 10"

	if strings.TrimSpace(sql) != expected {
		t.Errorf("expected sql %q, got %q", expected, sql)
	}
	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}
}

func TestPrint_RawQuery(t *testing.T) {
	m := New[TestModel]()
	m.Raw("SELECT * FROM users WHERE email = ?", "test@example.com")

	sql, args := m.Print()
	expected := "SELECT * FROM users WHERE email = ?"

	if sql != expected {
		t.Errorf("expected sql %q, got %q", expected, sql)
	}
	if len(args) != 1 || args[0] != "test@example.com" {
		t.Errorf("expected args ['test@example.com'], got %v", args)
	}
}

func TestPrint_WithFullText(t *testing.T) {
	m := New[TestModel]()
	m.WhereFullText("content", "search terms").Limit(20)

	sql, args := m.Print()
	expected := "SELECT * FROM test_models WHERE 1=1  AND (to_tsvector('english', content) @@ plainto_tsquery('english', ?)) LIMIT 20"

	if strings.TrimSpace(sql) != expected {
		t.Errorf("expected sql %q, got %q", expected, sql)
	}
	if len(args) != 1 {
		t.Errorf("expected 1 arg, got %d", len(args))
	}
}

func TestPrint_Chaining(t *testing.T) {
	m := New[TestModel]()

	// Build query step by step
	m.Where("status", "active")
	sql1, args1 := m.Print()

	// Chain more
	m.Where("age", 30)
	sql2, args2 := m.Print()

	// First print should have 1 WHERE
	if !strings.Contains(sql1, "status = ?") {
		t.Error("First print should contain status condition")
	}
	if len(args1) != 1 {
		t.Errorf("First print should have 1 arg, got %d", len(args1))
	}

	// Second print should have both WHEREs
	if !strings.Contains(sql2, "status = ?") || !strings.Contains(sql2, "age = ?") {
		t.Error("Second print should contain both conditions")
	}
	if len(args2) != 2 {
		t.Errorf("Second print should have 2 args, got %d", len(args2))
	}
}

// Example usage for documentation
func ExampleModel_Print() {
	m := New[TestModel]()
	m.Where("status", "active").Limit(10)

	sql, args := m.Print()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT * FROM test_models WHERE 1=1  AND (status = ?) LIMIT 10
	// [active]
}
