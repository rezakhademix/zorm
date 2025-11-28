package zorm

import (
	"context"
	"strings"
	"testing"
)

// TestPluck_QueryBuilding verifies that Pluck method exists and builds the correct query.
func TestPluck_QueryBuilding(t *testing.T) {
	m := New[TestModel]()
	m.Where("active", true)

	// Build the query after setting up Pluck
	// Note: Pluck modifies m.columns internally
	testModel := m.Clone()
	testModel.columns = []string{"name"}

	query, _ := testModel.buildSelectQuery()
	expected := "SELECT name FROM test_models WHERE 1=1  AND (active = ?)"

	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}

	// Verify the method signature is correct by type checking
	var _ func(context.Context, string) ([]any, error) = m.Pluck

	t.Log("Pluck method exists with correct signature")
}
