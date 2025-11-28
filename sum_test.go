package zorm

import (
	"context"
	"testing"
)

// TestSum_QueryBuilding verifies that Sum method exists and returns the correct type.
// Actual execution would require a database connection.
func TestSum_QueryBuilding(t *testing.T) {
	m := New[TestModel]()

	// Verify the method signature is correct by type checking
	// We can't execute without a DB connection, but we can verify the method exists
	var _ func(context.Context, string) (float64, error) = m.Sum

	// If we got here, the method signature is correct
	t.Log("Sum method exists with correct signature")
}
