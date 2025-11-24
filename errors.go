package zorm

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// Sentinel errors for common failure cases
var (
	// ErrRecordNotFound is returned when a query returns no results
	ErrRecordNotFound = errors.New("zorm: record not found")

	// ErrInvalidModel is returned when the model type is invalid
	ErrInvalidModel = errors.New("zorm: invalid model")

	// ErrNoContext is returned when no context is provided
	ErrNoContext = errors.New("zorm: no context provided")

	// ErrRelationNotFound is returned when a relation method is not found
	ErrRelationNotFound = errors.New("zorm: relation not found")

	// ErrInvalidRelation is returned when relation type is invalid
	ErrInvalidRelation = errors.New("zorm: invalid relation type")

	// ErrDuplicateKey is returned for unique constraint violations
	ErrDuplicateKey = errors.New("zorm: duplicate key violation")

	// ErrForeignKey is returned for foreign key constraint violations
	ErrForeignKey = errors.New("zorm: foreign key constraint violation")

	// ErrNilPointer is returned when a nil pointer is passed
	ErrNilPointer = errors.New("zorm: nil pointer")

	// ErrInvalidConfig is returned when relation config is invalid
	ErrInvalidConfig = errors.New("zorm: invalid relation config")

	// ErrRequiresRawQuery is returned when operation requires raw query
	ErrRequiresRawQuery = errors.New("zorm: operation requires raw query")
)

// QueryError wraps database errors with query context for better debugging
type QueryError struct {
	Query     string // The SQL query that failed
	Args      []any  // The query arguments
	Operation string // Operation type: SELECT, INSERT, UPDATE, DELETE
	Err       error  // The underlying error
}

func (e *QueryError) Error() string {
	argsStr := formatArgs(e.Args)
	return fmt.Sprintf("zorm: %s failed: %v\nQuery: %s\nArgs: %s",
		e.Operation, e.Err, e.Query, argsStr)
}

func (e *QueryError) Unwrap() error {
	return e.Err
}

// RelationError wraps relation loading failures with context
type RelationError struct {
	Relation  string // Name of the relation
	ModelType string // Type of the model
	Err       error  // The underlying error
}

func (e *RelationError) Error() string {
	return fmt.Sprintf("zorm: relation '%s' error on model %s: %v",
		e.Relation, e.ModelType, e.Err)
}

func (e *RelationError) Unwrap() error {
	return e.Err
}

// ValidationError represents a model validation failure
type ValidationError struct {
	Field   string // Field name that failed validation
	Value   any    // The invalid value
	Message string // Human-readable error message
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("zorm: validation failed for field '%s': %s (value: %v)",
		e.Field, e.Message, e.Value)
}

// WrapQueryError wraps a database error with query context
func WrapQueryError(operation, query string, args []any, err error) error {
	if err == nil {
		return nil
	}

	// Check for common database errors and wrap with sentinel
	if errors.Is(err, sql.ErrNoRows) {
		return ErrRecordNotFound
	}

	// Check for constraint violations
	errMsg := err.Error()
	if strings.Contains(errMsg, "duplicate key") ||
		strings.Contains(errMsg, "unique constraint") {
		return &QueryError{
			Query:     query,
			Args:      args,
			Operation: operation,
			Err:       fmt.Errorf("%w: %v", ErrDuplicateKey, err),
		}
	}

	if strings.Contains(errMsg, "foreign key") {
		return &QueryError{
			Query:     query,
			Args:      args,
			Operation: operation,
			Err:       fmt.Errorf("%w: %v", ErrForeignKey, err),
		}
	}

	return &QueryError{
		Query:     query,
		Args:      args,
		Operation: operation,
		Err:       err,
	}
}

// WrapRelationError wraps a relation error with context
func WrapRelationError(relation, modelType string, err error) error {
	if err == nil {
		return nil
	}
	return &RelationError{
		Relation:  relation,
		ModelType: modelType,
		Err:       err,
	}
}

// IsNotFound checks if the error is ErrRecordNotFound
func IsNotFound(err error) bool {
	return errors.Is(err, ErrRecordNotFound) || errors.Is(err, sql.ErrNoRows)
}

// IsConstraintViolation checks if the error is a constraint violation
func IsConstraintViolation(err error) bool {
	return errors.Is(err, ErrDuplicateKey) || errors.Is(err, ErrForeignKey)
}

// IsDuplicateKey checks if the error is a duplicate key violation
func IsDuplicateKey(err error) bool {
	return errors.Is(err, ErrDuplicateKey)
}

// IsForeignKeyViolation checks if the error is a foreign key violation
func IsForeignKeyViolation(err error) bool {
	return errors.Is(err, ErrForeignKey)
}

// formatArgs formats query arguments for error messages
func formatArgs(args []any) string {
	if len(args) == 0 {
		return "[]"
	}

	parts := make([]string, len(args))
	for i, arg := range args {
		parts[i] = fmt.Sprintf("%v", arg)
	}

	// Limit output length
	result := "[" + strings.Join(parts, ", ") + "]"
	if len(result) > 200 {
		return result[:197] + "...]"
	}
	return result
}
