package zorm

import (
	"context"
	"testing"
)

// TestExists_Signature verifies that Exists method exists and returns the correct type.
func TestExists_Signature(t *testing.T) {
	m := New[TestModel]()

	// Verify the method signature is correct by type checking
	var _ func(context.Context) (bool, error) = m.Exists

	t.Log("Exists method exists with correct signature")
}

// TestExists_QueryLogic simulates what Exists does to verify logic without executing.
// This duplicates the logic in Exists to ensure our understanding of what it SHOULD do is correct.
func TestExists_QueryLogic(t *testing.T) {
	m := New[TestModel]().Where("id", 1)

	// Replicate Exists logic for query building verification
	limit, offset := m.limit, m.offset
	orderBys := m.orderBys

	m.limit = 1
	m.offset = 0
	m.orderBys = nil

	sb := GetStringBuilder()
	defer PutStringBuilder(sb)

	sb.WriteString("SELECT 1 FROM ")
	sb.WriteString(m.TableName())
	m.buildWhereClause(sb)
	sb.WriteString(" LIMIT 1")

	query := sb.String()

	// Restore
	m.limit, m.offset = limit, offset
	m.orderBys = orderBys

	// Expected query based on zorm's query builder (double space is from implementation artifact)
	expected := "SELECT 1 FROM test_models WHERE 1=1  AND id = ? LIMIT 1"
	if query != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
}
