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

	aVal := reflect.ValueOf(a)
	bVal := reflect.ValueOf(b)

	// Handle pointers
	if aVal.Kind() == reflect.Pointer {
		if aVal.IsNil() {
			return b == nil
		}
		aVal = aVal.Elem()
	}
	if bVal.Kind() == reflect.Pointer {
		if bVal.IsNil() {
			return a == nil
		}
		bVal = bVal.Elem()
	}

	if !aVal.IsValid() || !bVal.IsValid() {
		return false
	}

	// Handle Integers
	if isInteger(aVal.Kind()) && isInteger(bVal.Kind()) {
		return aVal.Int() == bVal.Int()
	}

	// Handle Unsigned Integers
	if isUint(aVal.Kind()) && isUint(bVal.Kind()) {
		return aVal.Uint() == bVal.Uint()
	}

	// Handle Floats
	if isFloat(aVal.Kind()) && isFloat(bVal.Kind()) {
		return aVal.Float() == bVal.Float()
	}

	// Fallback to string comparison
	return fmt.Sprintf("%v", aVal.Interface()) == fmt.Sprintf("%v", bVal.Interface())
}

func isInteger(k reflect.Kind) bool {
	return k >= reflect.Int && k <= reflect.Int64
}

func isUint(k reflect.Kind) bool {
	return k >= reflect.Uint && k <= reflect.Uint64
}

func isFloat(k reflect.Kind) bool {
	return k == reflect.Float32 || k == reflect.Float64
}
