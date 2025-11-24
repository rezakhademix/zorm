package zorm

import (
	"fmt"
	"reflect"
)

// compareIDs compares two ID values, handling type conversions (int vs int64, etc.)
func compareIDs(a, b any) bool {
	if a == b {
		return true
	}

	// Try numeric comparison
	aVal := reflect.ValueOf(a)
	bVal := reflect.ValueOf(b)

	// Both must be valid
	if !aVal.IsValid() || !bVal.IsValid() {
		return false
	}

	// Handle numeric types (int, int64, int32, uint, etc.)
	if isNumeric(aVal.Kind()) && isNumeric(bVal.Kind()) {
		return aVal.Convert(reflect.TypeOf(int64(0))).Int() == bVal.Convert(reflect.TypeOf(int64(0))).Int()
	}

	// Fallback to string comparison
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// isNumeric checks if a reflect.Kind is a numeric type
func isNumeric(k reflect.Kind) bool {
	return k >= reflect.Int && k <= reflect.Float64
}
