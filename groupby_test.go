package zorm

import (
	"strings"
	"testing"
)

func TestGroupByRollup(t *testing.T) {
	m := New[TestModel]()
	m.Select("a", "b", "SUM(c)").GroupByRollup("a", "b")

	query, _ := m.buildSelectQuery()
	expected := "SELECT a, b, SUM(c) FROM test_models GROUP BY ROLLUP (a, b)"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
}

func TestGroupByCube(t *testing.T) {
	m := New[TestModel]()
	m.Select("a", "b", "SUM(c)").GroupByCube("a", "b")

	query, _ := m.buildSelectQuery()
	expected := "SELECT a, b, SUM(c) FROM test_models GROUP BY CUBE (a, b)"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
}

func TestGroupByGroupingSets(t *testing.T) {
	m := New[TestModel]()
	m.Select("brand", "size", "SUM(sales)")
	m.GroupByGroupingSets(
		[]string{"brand"},
		[]string{"size"},
		[]string{},
	)

	query, _ := m.buildSelectQuery()
	expected := "SELECT brand, size, SUM(sales) FROM test_models GROUP BY GROUPING SETS ((brand), (size), ())"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
}

func TestGroupByMixed(t *testing.T) {
	m := New[TestModel]()
	m.Select("*")
	m.GroupBy("a")
	m.GroupByRollup("b", "c")

	query, _ := m.buildSelectQuery()
	expected := "SELECT * FROM test_models GROUP BY a, ROLLUP (b, c)"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
}
