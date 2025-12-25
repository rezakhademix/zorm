package zorm

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

// =============================================================================
// ERROR HELPER FUNCTION TESTS
// =============================================================================

// TestIsNotFound verifies IsNotFound helper
func TestIsNotFound(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"ErrRecordNotFound", ErrRecordNotFound, true},
		{"sql.ErrNoRows", sql.ErrNoRows, true},
		{"wrapped ErrRecordNotFound", WrapQueryError("SELECT", "SELECT * FROM users", nil, ErrRecordNotFound), true},
		{"other error", errors.New("some error"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsNotFound(tt.err)
			if result != tt.expected {
				t.Errorf("IsNotFound(%v) = %v, want %v", tt.err, result, tt.expected)
			}
		})
	}
}

// TestIsDuplicateKey verifies IsDuplicateKey helper
func TestIsDuplicateKey(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"ErrDuplicateKey", ErrDuplicateKey, true},
		{"wrapped ErrDuplicateKey", WrapQueryError("INSERT", "INSERT INTO users...", nil, ErrDuplicateKey), true},
		{"other error", errors.New("some error"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsDuplicateKey(tt.err)
			if result != tt.expected {
				t.Errorf("IsDuplicateKey(%v) = %v, want %v", tt.err, result, tt.expected)
			}
		})
	}
}

// TestIsForeignKeyViolation verifies IsForeignKeyViolation helper
func TestIsForeignKeyViolation(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"ErrForeignKey", ErrForeignKey, true},
		{"other error", errors.New("some error"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsForeignKeyViolation(tt.err)
			if result != tt.expected {
				t.Errorf("IsForeignKeyViolation(%v) = %v, want %v", tt.err, result, tt.expected)
			}
		})
	}
}

// TestIsNotNullViolation verifies IsNotNullViolation helper
func TestIsNotNullViolation(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"ErrNotNullViolation", ErrNotNullViolation, true},
		{"other error", errors.New("some error"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsNotNullViolation(tt.err)
			if result != tt.expected {
				t.Errorf("IsNotNullViolation(%v) = %v, want %v", tt.err, result, tt.expected)
			}
		})
	}
}

// TestIsCheckViolation verifies IsCheckViolation helper
func TestIsCheckViolation(t *testing.T) {
	if IsCheckViolation(nil) {
		t.Error("IsCheckViolation(nil) should be false")
	}
	if !IsCheckViolation(ErrCheckViolation) {
		t.Error("IsCheckViolation(ErrCheckViolation) should be true")
	}
}

// TestIsDeadlock verifies IsDeadlock helper
func TestIsDeadlock(t *testing.T) {
	if IsDeadlock(nil) {
		t.Error("IsDeadlock(nil) should be false")
	}
	if !IsDeadlock(ErrTransactionDeadlock) {
		t.Error("IsDeadlock(ErrTransactionDeadlock) should be true")
	}
}

// TestIsSerializationFailure verifies IsSerializationFailure helper
func TestIsSerializationFailure(t *testing.T) {
	if IsSerializationFailure(nil) {
		t.Error("IsSerializationFailure(nil) should be false")
	}
	if !IsSerializationFailure(ErrSerializationFailure) {
		t.Error("IsSerializationFailure(ErrSerializationFailure) should be true")
	}
}

// TestIsConnectionError verifies IsConnectionError helper
func TestIsConnectionError(t *testing.T) {
	if IsConnectionError(nil) {
		t.Error("IsConnectionError(nil) should be false")
	}
	if !IsConnectionError(ErrConnectionFailed) {
		t.Error("IsConnectionError(ErrConnectionFailed) should be true")
	}
	if !IsConnectionError(ErrConnectionLost) {
		t.Error("IsConnectionError(ErrConnectionLost) should be true")
	}
}

// TestIsTimeout verifies IsTimeout helper
func TestIsTimeout(t *testing.T) {
	if IsTimeout(nil) {
		t.Error("IsTimeout(nil) should be false")
	}
	if !IsTimeout(ErrTimeout) {
		t.Error("IsTimeout(ErrTimeout) should be true")
	}
	if !IsTimeout(context.DeadlineExceeded) {
		t.Error("IsTimeout(context.DeadlineExceeded) should be true")
	}
}

// TestIsSchemaError verifies IsSchemaError helper
func TestIsSchemaError(t *testing.T) {
	if IsSchemaError(nil) {
		t.Error("IsSchemaError(nil) should be false")
	}
	if !IsSchemaError(ErrColumnNotFound) {
		t.Error("IsSchemaError(ErrColumnNotFound) should be true")
	}
	if !IsSchemaError(ErrTableNotFound) {
		t.Error("IsSchemaError(ErrTableNotFound) should be true")
	}
	if !IsSchemaError(ErrInvalidSyntax) {
		t.Error("IsSchemaError(ErrInvalidSyntax) should be true")
	}
}

// TestIsConstraintViolation verifies IsConstraintViolation helper
func TestIsConstraintViolation(t *testing.T) {
	if IsConstraintViolation(nil) {
		t.Error("IsConstraintViolation(nil) should be false")
	}
	// All constraint errors should return true
	constraintErrors := []error{
		ErrDuplicateKey,
		ErrForeignKey,
		ErrNotNullViolation,
		ErrCheckViolation,
	}
	for _, err := range constraintErrors {
		if !IsConstraintViolation(err) {
			t.Errorf("IsConstraintViolation(%v) should be true", err)
		}
	}
}

// =============================================================================
// QUERYERROR TESTS
// =============================================================================

// TestQueryError_Error verifies QueryError.Error method
func TestQueryError_Error(t *testing.T) {
	qe := &QueryError{
		Err:       ErrRecordNotFound,
		Query:     "SELECT * FROM users WHERE id = $1",
		Args:      []any{1},
		Operation: "SELECT",
		Table:     "users",
	}

	errStr := qe.Error()
	if errStr == "" {
		t.Error("QueryError.Error() should return non-empty string")
	}
}

// TestQueryError_Unwrap verifies QueryError.Unwrap method
func TestQueryError_Unwrap(t *testing.T) {
	originalErr := ErrRecordNotFound
	qe := &QueryError{
		Err:   originalErr,
		Query: "SELECT * FROM users",
	}

	unwrapped := qe.Unwrap()
	if unwrapped != originalErr {
		t.Errorf("Unwrap should return original error, got %v", unwrapped)
	}
}

// TestGetQueryError verifies GetQueryError helper
func TestGetQueryError(t *testing.T) {
	qe := &QueryError{
		Err:       ErrRecordNotFound,
		Query:     "SELECT * FROM users",
		Operation: "SELECT",
	}

	// Should extract QueryError from wrapped error
	result := GetQueryError(qe)
	if result == nil {
		t.Error("GetQueryError should return QueryError")
	}

	// Non-QueryError should return nil
	result2 := GetQueryError(errors.New("regular error"))
	if result2 != nil {
		t.Error("GetQueryError should return nil for non-QueryError")
	}
}

// TestWrapQueryError verifies WrapQueryError function
func TestWrapQueryError(t *testing.T) {
	originalErr := errors.New("database error")
	wrapped := WrapQueryError("SELECT", "SELECT * FROM users", []any{1}, originalErr)

	if wrapped == nil {
		t.Fatal("WrapQueryError should return non-nil error")
	}

	qe := GetQueryError(wrapped)
	if qe == nil {
		t.Fatal("wrapped error should be extractable as QueryError")
	}

	if qe.Operation != "SELECT" {
		t.Errorf("Operation should be SELECT, got %q", qe.Operation)
	}
}

// =============================================================================
// RELATIONERROR TESTS
// =============================================================================

// TestRelationError_Error verifies RelationError.Error method
func TestRelationError_Error(t *testing.T) {
	re := &RelationError{
		Err:       ErrRelationNotFound,
		Relation:  "Posts",
		ModelType: "User",
	}

	errStr := re.Error()
	if errStr == "" {
		t.Error("RelationError.Error() should return non-empty string")
	}
}

// TestRelationError_Unwrap verifies RelationError.Unwrap method
func TestRelationError_Unwrap(t *testing.T) {
	originalErr := ErrRelationNotFound
	re := &RelationError{
		Err:      originalErr,
		Relation: "Posts",
	}

	unwrapped := re.Unwrap()
	if unwrapped != originalErr {
		t.Errorf("Unwrap should return original error, got %v", unwrapped)
	}
}

// TestWrapRelationError verifies WrapRelationError function
func TestWrapRelationError(t *testing.T) {
	originalErr := ErrRelationNotFound
	wrapped := WrapRelationError("Posts", "User", originalErr)

	if wrapped == nil {
		t.Fatal("WrapRelationError should return non-nil error")
	}

	var re *RelationError
	if errors.As(wrapped, &re) {
		if re.Relation != "Posts" {
			t.Errorf("Relation should be Posts, got %q", re.Relation)
		}
		if re.ModelType != "User" {
			t.Errorf("ModelType should be User, got %q", re.ModelType)
		}
	} else {
		t.Error("wrapped error should be extractable as RelationError")
	}
}

// =============================================================================
// SENTINEL ERROR TESTS
// =============================================================================

// TestSentinelErrors verifies all sentinel errors are defined
func TestSentinelErrors(t *testing.T) {
	sentinelErrors := []struct {
		name string
		err  error
	}{
		{"ErrRecordNotFound", ErrRecordNotFound},
		{"ErrInvalidModel", ErrInvalidModel},
		{"ErrNilPointer", ErrNilPointer},
		{"ErrRelationNotFound", ErrRelationNotFound},
		{"ErrInvalidRelation", ErrInvalidRelation},
		{"ErrDuplicateKey", ErrDuplicateKey},
		{"ErrForeignKey", ErrForeignKey},
		{"ErrNotNullViolation", ErrNotNullViolation},
		{"ErrCheckViolation", ErrCheckViolation},
		{"ErrConnectionFailed", ErrConnectionFailed},
		{"ErrConnectionLost", ErrConnectionLost},
		{"ErrTimeout", ErrTimeout},
		{"ErrTransactionDeadlock", ErrTransactionDeadlock},
		{"ErrSerializationFailure", ErrSerializationFailure},
		{"ErrColumnNotFound", ErrColumnNotFound},
		{"ErrTableNotFound", ErrTableNotFound},
		{"ErrInvalidSyntax", ErrInvalidSyntax},
		{"ErrInvalidColumnName", ErrInvalidColumnName},
	}

	for _, se := range sentinelErrors {
		t.Run(se.name, func(t *testing.T) {
			if se.err == nil {
				t.Errorf("%s should not be nil", se.name)
			}
			if se.err.Error() == "" {
				t.Errorf("%s.Error() should not be empty", se.name)
			}
		})
	}
}
