package zorm

import (
	"fmt"
	"reflect"
)

// compareIDs compares two ID values, handling type conversions (int vs int64, etc.)
func compareIDs(a, b any) bool {
	// Fast path: direct equality check (handles same type comparisons)
	if a == b {
		return true
	}

	// Handle nil cases early
	if a == nil || b == nil {
		return a == b
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

	aKind := aVal.Kind()
	bKind := bVal.Kind()

	// Handle Arrays (UUIDs are [16]byte arrays)
	// This is MUCH faster than string comparison for uuid.UUID
	if aKind == reflect.Array && bKind == reflect.Array {
		if aVal.Type() == bVal.Type() && aVal.Len() == bVal.Len() {
			// Byte-by-byte comparison for arrays (like UUID)
			for i := 0; i < aVal.Len(); i++ {
				if aVal.Index(i).Interface() != bVal.Index(i).Interface() {
					return false
				}
			}
			return true
		}
	}

	// Handle Integers
	if isInteger(aKind) && isInteger(bKind) {
		return aVal.Int() == bVal.Int()
	}

	// Handle Unsigned Integers
	if isUint(aKind) && isUint(bKind) {
		return aVal.Uint() == bVal.Uint()
	}

	// Handle mixed signed/unsigned (for completeness)
	if isInteger(aKind) && isUint(bKind) {
		aInt := aVal.Int()
		if aInt < 0 {
			return false
		}
		return uint64(aInt) == bVal.Uint()
	}
	if isUint(aKind) && isInteger(bKind) {
		bInt := bVal.Int()
		if bInt < 0 {
			return false
		}
		return aVal.Uint() == uint64(bInt)
	}

	// Handle Floats
	if isFloat(aKind) && isFloat(bKind) {
		return aVal.Float() == bVal.Float()
	}

	// Handle Strings (fast path for string UUIDs)
	if aKind == reflect.String && bKind == reflect.String {
		return aVal.String() == bVal.String()
	}

	// Fallback to string comparison (slower, but handles edge cases)
	// This allocates memory for string conversion
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
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
