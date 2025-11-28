package zorm

import (
	"strings"
	"testing"
)

func TestDistinct(t *testing.T) {
	m := New[TestModel]()
	m.Select("city").Distinct()

	query, _ := m.buildSelectQuery()
	expected := "SELECT DISTINCT city FROM test_models"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
}

func TestDistinctBy(t *testing.T) {
	m := New[TestModel]()
	m.Select("city", "time", "report").DistinctBy("city").OrderBy("city", "ASC").OrderBy("time", "DESC")

	query, _ := m.buildSelectQuery()
	expected := "SELECT DISTINCT ON (city) city, time, report FROM test_models ORDER BY city ASC, time DESC"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
}
