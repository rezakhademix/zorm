package zorm

import (
	"strings"
	"testing"
)

func TestWithCTE_RawString(t *testing.T) {
	m := New[TestModel]()
	m.WithCTE("cte", "SELECT * FROM users WHERE age > 18")
	m.Select("*")

	query, args := m.buildSelectQuery()
	expected := "WITH cte AS (SELECT * FROM users WHERE age > 18) SELECT * FROM test_models"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
	if len(args) != 0 {
		t.Errorf("expected 0 args, got %d", len(args))
	}
}

func TestWithCTE_SubQuery(t *testing.T) {
	sub := New[TestModel]()
	sub.Where("age", ">", 18)

	m := New[TestModel]()
	m.WithCTE("cte", sub)
	m.Select("*").Where("id IN (SELECT id FROM cte)")

	query, args := m.buildSelectQuery()
	// The subquery builder generates "SELECT * FROM test_models WHERE age > ?"
	// Note: buildSelectQuery adds "SELECT * FROM test_models" by default if no columns selected,
	// but here sub has no columns selected so it defaults to *.

	expected := "WITH cte AS (SELECT * FROM test_models WHERE 1=1  AND age > ?) SELECT * FROM test_models WHERE 1=1  AND id IN (SELECT id FROM cte)"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}

	if len(args) != 1 {
		t.Errorf("expected 1 arg, got %d", len(args))
	}
	if args[0] != 18 {
		t.Errorf("expected arg 18, got %v", args[0])
	}
}

func TestWithCTE_Multiple(t *testing.T) {
	m := New[TestModel]()
	m.WithCTE("cte1", "SELECT 1")
	m.WithCTE("cte2", "SELECT 2")
	m.Select("*")

	query, _ := m.buildSelectQuery()
	expected := "WITH cte1 AS (SELECT 1), cte2 AS (SELECT 2) SELECT * FROM test_models"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
}

func TestWithCTE_And_EagerLoad(t *testing.T) {
	m := New[TestModel]()
	m.WithCTE("cte", "SELECT * FROM users WHERE active = 1")
	m.With("Profile") // Eager load Profile
	m.Select("*")

	query, _ := m.buildSelectQuery()
	expected := "WITH cte AS (SELECT * FROM users WHERE active = 1) SELECT * FROM test_models"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}

	// Verify relations are set
	if len(m.relations) != 1 || m.relations[0] != "Profile" {
		t.Errorf("expected relations [Profile], got %v", m.relations)
	}
}
