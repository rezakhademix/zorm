package zorm

import (
	"strings"
	"testing"
)

func TestWhereFullText(t *testing.T) {
	m := New[TestModel]()
	m.WhereFullText("content", "search terms")

	query, args := m.buildSelectQuery()
	expected := "SELECT * FROM test_models WHERE 1=1  AND (to_tsvector('english', content) @@ plainto_tsquery('english', ?))"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
	if len(args) != 1 || args[0] != "search terms" {
		t.Errorf("expected args ['search terms'], got %v", args)
	}
}

func TestWhereFullTextWithConfig(t *testing.T) {
	m := New[TestModel]()
	m.WhereFullTextWithConfig("content", "buscar términos", "spanish")

	query, args := m.buildSelectQuery()
	expected := "SELECT * FROM test_models WHERE 1=1  AND (to_tsvector('spanish', content) @@ plainto_tsquery('spanish', ?))"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
	if len(args) != 1 || args[0] != "buscar términos" {
		t.Errorf("expected args ['buscar términos'], got %v", args)
	}
}

func TestWhereTsVector(t *testing.T) {
	m := New[TestModel]()
	m.WhereTsVector("search_vector", "fat & rat")

	query, args := m.buildSelectQuery()
	expected := "SELECT * FROM test_models WHERE 1=1  AND (search_vector @@ to_tsquery('english', ?))"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
	if len(args) != 1 || args[0] != "fat & rat" {
		t.Errorf("expected args ['fat & rat'], got %v", args)
	}
}

func TestWherePhraseSearch(t *testing.T) {
	m := New[TestModel]()
	m.WherePhraseSearch("title", "fat cat")

	query, args := m.buildSelectQuery()
	expected := "SELECT * FROM test_models WHERE 1=1  AND (to_tsvector('english', title) @@ phraseto_tsquery('english', ?))"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
	if len(args) != 1 || args[0] != "fat cat" {
		t.Errorf("expected args ['fat cat'], got %v", args)
	}
}

func TestWhereFullText_Combined(t *testing.T) {
	m := New[TestModel]()
	m.Where("published", true).WhereFullText("content", "postgresql").Limit(10)

	query, args := m.buildSelectQuery()
	expected := "SELECT * FROM test_models WHERE 1=1  AND (published = ?) AND (to_tsvector('english', content) @@ plainto_tsquery('english', ?)) LIMIT 10"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
	if len(args) != 2 || args[0] != true || args[1] != "postgresql" {
		t.Errorf("expected args [true, 'postgresql'], got %v", args)
	}
}
