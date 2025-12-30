package zorm

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/google/uuid"
)

// compareIDs compares two ID values, handling type conversions (int vs int64, UUIDs, etc.)
func compareIDs(a, b any) bool {
	// Fast path: direct equality check (handles same type comparisons)
	if a == b {
		return true
	}

	// Handle nil cases early
	if a == nil || b == nil {
		return a == b
	}

	// Handle UUID comparisons early (most common case in your app)
	// This avoids reflection overhead for the common case
	switch aVal := a.(type) {
	case uuid.UUID:
		switch bVal := b.(type) {
		case uuid.UUID:
			return aVal == bVal
		case string:
			if bUUID, err := uuid.Parse(bVal); err == nil {
				return aVal == bUUID
			}
			return false
		case []byte:
			if bUUID, err := uuid.ParseBytes(bVal); err == nil {
				return aVal == bUUID
			}
			return false
		}
	case string:
		if bVal, ok := b.(uuid.UUID); ok {
			if aUUID, err := uuid.Parse(aVal); err == nil {
				return aUUID == bVal
			}
			return false
		}
		// String-to-string comparison (fast path)
		if bStr, ok := b.(string); ok {
			return aVal == bStr
		}
	case []byte:
		if bVal, ok := b.(uuid.UUID); ok {
			if aUUID, err := uuid.ParseBytes(aVal); err == nil {
				return aUUID == bVal
			}
			return false
		}
	}

	// Now use reflection for other types
	aVal := reflect.ValueOf(a)
	bVal := reflect.ValueOf(b)

	// Handle pointers
	if aVal.Kind() == reflect.Pointer {
		if aVal.IsNil() {
			return false
		}
		aVal = aVal.Elem()
		a = aVal.Interface()
	}
	if bVal.Kind() == reflect.Pointer {
		if bVal.IsNil() {
			return false
		}
		bVal = bVal.Elem()
		b = bVal.Interface()
	}

	if !aVal.IsValid() || !bVal.IsValid() {
		return false
	}

	aKind := aVal.Kind()
	bKind := bVal.Kind()

	// Handle Arrays (for [16]byte UUID representation)
	if aKind == reflect.Array && bKind == reflect.Array {
		if aVal.Type() == bVal.Type() {
			// Fast comparison for same-type arrays
			if aVal.Type().Elem().Kind() == reflect.Uint8 {
				// Optimize byte array comparison using unsafe
				return compareBytesUnsafe(a, b, aVal.Len())
			}
			// Generic array comparison
			for i := 0; i < aVal.Len(); i++ {
				if aVal.Index(i).Interface() != bVal.Index(i).Interface() {
					return false
				}
			}
			return true
		}
	}

	// Handle Integers (signed)
	if isInteger(aKind) && isInteger(bKind) {
		return aVal.Int() == bVal.Int()
	}

	// Handle Unsigned Integers
	if isUint(aKind) && isUint(bKind) {
		return aVal.Uint() == bVal.Uint()
	}

	// Handle mixed signed/unsigned
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

	// Handle Strings
	if aKind == reflect.String && bKind == reflect.String {
		return aVal.String() == bVal.String()
	}

	// Last resort: string comparison (slow, avoid if possible)
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// compareBytesUnsafe uses unsafe pointer comparison for byte arrays
// This is faster than element-by-element comparison
func compareBytesUnsafe(a, b any, length int) bool {
	aPtr := (*[16]byte)(unsafe.Pointer(reflect.ValueOf(a).UnsafeAddr()))
	bPtr := (*[16]byte)(unsafe.Pointer(reflect.ValueOf(b).UnsafeAddr()))
	return *aPtr == *bPtr
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
