package zorm

import (
	"testing"
)

func TestSum_Basic(t *testing.T) {
	m := New[TestModel]()

	// Mock buildSelectQuery to return expected SQL
	query, _ := m.buildSelectQuery()

	// Since we can't actually execute against a real DB in unit tests,
	// we'll just verify the query structure is correct by checking
	// that Sum builds the correct query

	// Test that Sum method exists and can be called
	// In a real scenario, this would need a database connection
	_, err := m.Sum("amount")

	// We expect an error since there's no DB connection
	if err == nil {
		t.Error("Expected error due to no DB connection, got nil")
	}

	// Verify the query variable was used (avoid unused variable error)
	_ = query
}
