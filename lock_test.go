package zorm

import (
	"strings"
	"testing"
)

func TestLock_ForUpdate(t *testing.T) {
	m := New[TestModel]()
	m.Where("id", 1).Lock("UPDATE")

	query, _ := m.buildSelectQuery()
	expected := "SELECT * FROM test_models WHERE 1=1  AND id = ? FOR UPDATE"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
}

func TestLock_ForShare(t *testing.T) {
	m := New[TestModel]()
	m.Select("*").Lock("SHARE")

	query, _ := m.buildSelectQuery()
	expected := "SELECT * FROM test_models FOR SHARE"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
}

func TestLock_ForNoKeyUpdate(t *testing.T) {
	m := New[TestModel]()
	m.Where("status", "active").Lock("NO KEY UPDATE")

	query, _ := m.buildSelectQuery()
	expected := "SELECT * FROM test_models WHERE 1=1  AND status = ? FOR NO KEY UPDATE"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
}
