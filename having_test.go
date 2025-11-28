package zorm

import (
	"strings"
	"testing"
)

func TestHaving_Aggregate(t *testing.T) {
	m := New[TestModel]()
	m.Select("city", "COUNT(*)").GroupBy("city").Having("COUNT(*) > ?", 1)

	query, args := m.buildSelectQuery()
	expected := "SELECT city, COUNT(*) FROM test_models GROUP BY city HAVING COUNT(*) > ?"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
	if len(args) != 1 || args[0] != 1 {
		t.Errorf("expected args [1], got %v", args)
	}
}

func TestHaving_NonAggregate(t *testing.T) {
	m := New[TestModel]()
	m.Select("x", "SUM(y)").GroupBy("x").Having("x < ?", "c")

	query, args := m.buildSelectQuery()
	expected := "SELECT x, SUM(y) FROM test_models GROUP BY x HAVING x < ?"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
	if len(args) != 1 || args[0] != "c" {
		t.Errorf("expected args ['c'], got %v", args)
	}
}

func TestHaving_OperatorInference(t *testing.T) {
	m := New[TestModel]()
	m.Select("city", "MAX(temp)").GroupBy("city").Having("MAX(temp) <", 40)

	query, args := m.buildSelectQuery()
	expected := "SELECT city, MAX(temp) FROM test_models GROUP BY city HAVING MAX(temp) < ?"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
	if len(args) != 1 || args[0] != 40 {
		t.Errorf("expected args [40], got %v", args)
	}
}

func TestHaving_Multiple(t *testing.T) {
	m := New[TestModel]()
	m.Select("*").GroupBy("city")
	m.Having("COUNT(*) > 5")
	m.Having("MAX(temp) < 100")

	query, _ := m.buildSelectQuery()
	expected := "SELECT * FROM test_models GROUP BY city HAVING COUNT(*) > 5 AND MAX(temp) < 100"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
}
