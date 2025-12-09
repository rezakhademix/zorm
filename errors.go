package zorm

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// Sentinel errors for common failure cases
var (
	// ErrRecordNotFound Query errors
	// ErrRecordNotFound is returned when a query returns no results
	ErrRecordNotFound = errors.New("zorm: record not found")

	// ErrInvalidModel Model errors
	// ErrInvalidModel is returned when the model type is invalid
	ErrInvalidModel = errors.New("zorm: invalid model")
	// ErrNilPointer is returned when a nil pointer is passed
	ErrNilPointer = errors.New("zorm: nil pointer")

	// ErrNoContext Context errors
	// ErrNoContext is returned when no context is provided
	ErrNoContext = errors.New("zorm: no context provided")

	// Relation errors
	// ErrRelationNotFound is returned when a relation method is not found
	ErrRelationNotFound = errors.New("zorm: relation not found")
	// ErrInvalidRelation is returned when relation type is invalid
	ErrInvalidRelation = errors.New("zorm: invalid relation type")
	// ErrInvalidConfig is returned when relation config is invalid
	ErrInvalidConfig = errors.New("zorm: invalid relation config")

	// ErrDuplicateKey Constraint violation errors
	// ErrDuplicateKey is returned for unique constraint violations
	ErrDuplicateKey = errors.New("zorm: duplicate key violation")
	// ErrForeignKey is returned for foreign key constraint violations
	ErrForeignKey = errors.New("zorm: foreign key constraint violation")
	// ErrCheckViolation is returned for CHECK constraint violations
	ErrCheckViolation = errors.New("zorm: check constraint violation")
	// ErrNotNullViolation is returned for NOT NULL constraint violations
	ErrNotNullViolation = errors.New("zorm: not null constraint violation")

	// Connection errors
	// ErrConnectionFailed is returned when database connection fails
	ErrConnectionFailed = errors.New("zorm: connection failed")
	// ErrConnectionLost is returned when connection is lost during operation
	ErrConnectionLost = errors.New("zorm: connection lost")
	// ErrTimeout is returned when a query or connection times out
	ErrTimeout = errors.New("zorm: operation timeout")

	// ErrTransactionDeadlock Transaction errors
	// ErrTransactionDeadlock is returned when a deadlock is detected
	ErrTransactionDeadlock = errors.New("zorm: transaction deadlock")
	// ErrSerializationFailure is returned for serialization failures
	ErrSerializationFailure = errors.New("zorm: serialization failure")

	// ErrColumnNotFound Schema errors
	// ErrColumnNotFound is returned when a column doesn't exist
	ErrColumnNotFound = errors.New("zorm: column not found")
	// ErrTableNotFound is returned when a table doesn't exist
	ErrTableNotFound = errors.New("zorm: table not found")
	// ErrInvalidSyntax is returned for SQL syntax errors
	ErrInvalidSyntax = errors.New("zorm: invalid SQL syntax")

	// ErrRequiresRawQuery Other errors
	// ErrRequiresRawQuery is returned when operation requires raw query
	ErrRequiresRawQuery = errors.New("zorm: operation requires raw query")
)

// QueryError wraps database errors with query context for better debugging.
// It provides detailed information about what went wrong including the query,
// arguments, operation type, and optionally the affected table and constraint.
type QueryError struct {
	Query      string // The SQL query that failed
	Args       []any  // The query arguments
	Operation  string // Operation type: SELECT, INSERT, UPDATE, DELETE, etc.
	Err        error  // The underlying error
	Table      string // The table involved (if detectable)
	Constraint string // The constraint name (if constraint violation)
}

func (e *QueryError) Error() string {
	argsStr := formatArgs(e.Args)
	var msg strings.Builder
	msg.WriteString(fmt.Sprintf("zorm: %s failed: %v", e.Operation, e.Err))

	if e.Table != "" {
		msg.WriteString(fmt.Sprintf("\nTable: %s", e.Table))
	}
	if e.Constraint != "" {
		msg.WriteString(fmt.Sprintf("\nConstraint: %s", e.Constraint))
	}

	msg.WriteString(fmt.Sprintf("\nQuery: %s", e.Query))
	msg.WriteString(fmt.Sprintf("\nArgs: %s", argsStr))

	return msg.String()
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

// WrapQueryError wraps a database error with query context.
// It analyzes the error to categorize it and extract relevant information
// such as table names and constraint names where possible.
func WrapQueryError(operation, query string, args []any, err error) error {
	if err == nil {
		return nil
	}

	// Check for common database errors and wrap with sentinel
	if errors.Is(err, sql.ErrNoRows) {
		return ErrRecordNotFound
	}

	// Convert error to lowercase for case-insensitive matching
	errMsg := strings.ToLower(err.Error())

	// Detect constraint violations
	if isUniqueViolation(errMsg) {
		qe := &QueryError{
			Query:     query,
			Args:      args,
			Operation: operation,
			Err:       fmt.Errorf("%w: %v", ErrDuplicateKey, err),
		}
		extractConstraintInfo(qe, err.Error())
		return qe
	}

	if isForeignKeyViolation(errMsg) {
		qe := &QueryError{
			Query:     query,
			Args:      args,
			Operation: operation,
			Err:       fmt.Errorf("%w: %v", ErrForeignKey, err),
		}
		extractConstraintInfo(qe, err.Error())
		return qe
	}

	if isNotNullViolation(errMsg) {
		qe := &QueryError{
			Query:     query,
			Args:      args,
			Operation: operation,
			Err:       fmt.Errorf("%w: %v", ErrNotNullViolation, err),
		}
		extractConstraintInfo(qe, err.Error())
		return qe
	}

	if isCheckViolation(errMsg) {
		qe := &QueryError{
			Query:     query,
			Args:      args,
			Operation: operation,
			Err:       fmt.Errorf("%w: %v", ErrCheckViolation, err),
		}
		extractConstraintInfo(qe, err.Error())
		return qe
	}

	// Detect deadlock
	if isDeadlock(errMsg) {
		return &QueryError{
			Query:     query,
			Args:      args,
			Operation: operation,
			Err:       fmt.Errorf("%w: %v", ErrTransactionDeadlock, err),
		}
	}

	// Detect serialization failure
	if isSerializationFailure(errMsg) {
		return &QueryError{
			Query:     query,
			Args:      args,
			Operation: operation,
			Err:       fmt.Errorf("%w: %v", ErrSerializationFailure, err),
		}
	}

	// Detect connection errors
	if isConnectionError(errMsg) {
		return &QueryError{
			Query:     query,
			Args:      args,
			Operation: operation,
			Err:       fmt.Errorf("%w: %v", ErrConnectionFailed, err),
		}
	}

	if isConnectionLost(errMsg) {
		return &QueryError{
			Query:     query,
			Args:      args,
			Operation: operation,
			Err:       fmt.Errorf("%w: %v", ErrConnectionLost, err),
		}
	}

	if isTimeout(errMsg) {
		return &QueryError{
			Query:     query,
			Args:      args,
			Operation: operation,
			Err:       fmt.Errorf("%w: %v", ErrTimeout, err),
		}
	}

	// Detect schema errors
	if isColumnNotFound(errMsg) {
		return &QueryError{
			Query:     query,
			Args:      args,
			Operation: operation,
			Err:       fmt.Errorf("%w: %v", ErrColumnNotFound, err),
		}
	}

	if isTableNotFound(errMsg) {
		return &QueryError{
			Query:     query,
			Args:      args,
			Operation: operation,
			Err:       fmt.Errorf("%w: %v", ErrTableNotFound, err),
		}
	}

	if isSyntaxError(errMsg) {
		return &QueryError{
			Query:     query,
			Args:      args,
			Operation: operation,
			Err:       fmt.Errorf("%w: %v", ErrInvalidSyntax, err),
		}
	}

	// Generic query error
	return &QueryError{
		Query:     query,
		Args:      args,
		Operation: operation,
		Err:       err,
	}
}

// Error detection helpers - these check for patterns across different database systems

// isUniqueViolation checks if the error is a unique constraint violation.
// Supports PostgreSQL, MySQL, and SQLite patterns.
func isUniqueViolation(errMsg string) bool {
	return strings.Contains(errMsg, "duplicate key") ||
		strings.Contains(errMsg, "unique constraint") ||
		strings.Contains(errMsg, "violates unique") ||
		strings.Contains(errMsg, "duplicate entry") || // MySQL
		strings.Contains(errMsg, "unique index") // SQLite
}

// isForeignKeyViolation checks if the error is a foreign key constraint violation.
func isForeignKeyViolation(errMsg string) bool {
	return strings.Contains(errMsg, "foreign key") ||
		strings.Contains(errMsg, "violates foreign key") ||
		strings.Contains(errMsg, "cannot add or update a child row") || // MySQL
		strings.Contains(errMsg, "cannot delete or update a parent row")
}

// isNotNullViolation checks if the error is a NOT NULL constraint violation.
func isNotNullViolation(errMsg string) bool {
	return strings.Contains(errMsg, "not null") ||
		strings.Contains(errMsg, "violates not-null") ||
		strings.Contains(errMsg, "cannot be null") || // MySQL
		strings.Contains(errMsg, "may not be null")
}

// isCheckViolation checks if the error is a CHECK constraint violation.
func isCheckViolation(errMsg string) bool {
	return strings.Contains(errMsg, "check constraint") ||
		strings.Contains(errMsg, "violates check") ||
		strings.Contains(errMsg, "constraint failed") // SQLite
}

// isDeadlock checks if the error is a transaction deadlock.
func isDeadlock(errMsg string) bool {
	return strings.Contains(errMsg, "deadlock") ||
		strings.Contains(errMsg, "lock timeout") ||
		strings.Contains(errMsg, "deadlock detected")
}

// isSerializationFailure checks if the error is a serialization failure.
func isSerializationFailure(errMsg string) bool {
	return strings.Contains(errMsg, "serialization failure") ||
		strings.Contains(errMsg, "could not serialize") ||
		strings.Contains(errMsg, "serializable isolation") ||
		strings.Contains(errMsg, "retry transaction")
}

// isConnectionError checks if the error is a connection failure.
func isConnectionError(errMsg string) bool {
	return strings.Contains(errMsg, "connection refused") ||
		strings.Contains(errMsg, "unable to connect") ||
		strings.Contains(errMsg, "connection failed") ||
		strings.Contains(errMsg, "no connection") ||
		strings.Contains(errMsg, "connect: connection refused")
}

// isConnectionLost checks if the error indicates a lost connection.
func isConnectionLost(errMsg string) bool {
	return strings.Contains(errMsg, "connection reset") ||
		strings.Contains(errMsg, "connection lost") ||
		strings.Contains(errMsg, "broken pipe") ||
		strings.Contains(errMsg, "eof") ||
		strings.Contains(errMsg, "server closed")
}

// isTimeout checks if the error is a timeout.
func isTimeout(errMsg string) bool {
	return strings.Contains(errMsg, "timeout") ||
		strings.Contains(errMsg, "timed out") ||
		strings.Contains(errMsg, "deadline exceeded") ||
		strings.Contains(errMsg, "context deadline exceeded")
}

// isColumnNotFound checks if the error indicates a missing column.
func isColumnNotFound(errMsg string) bool {
	return strings.Contains(errMsg, "column") && strings.Contains(errMsg, "does not exist") ||
		strings.Contains(errMsg, "unknown column") || // MySQL
		strings.Contains(errMsg, "no such column") // SQLite
}

// isTableNotFound checks if the error indicates a missing table.
func isTableNotFound(errMsg string) bool {
	return strings.Contains(errMsg, "table") && strings.Contains(errMsg, "does not exist") ||
		strings.Contains(errMsg, "relation") && strings.Contains(errMsg, "does not exist") || // PostgreSQL
		strings.Contains(errMsg, "no such table") || // SQLite
		strings.Contains(errMsg, "table") && strings.Contains(errMsg, "doesn't exist") // MySQL
}

// isSyntaxError checks if the error is a SQL syntax error.
func isSyntaxError(errMsg string) bool {
	return strings.Contains(errMsg, "syntax error") ||
		strings.Contains(errMsg, "syntax") && strings.Contains(errMsg, "near") ||
		strings.Contains(errMsg, "you have an error in your sql syntax") // MySQL
}

// extractConstraintInfo attempts to extract table and constraint names from error messages.
// This uses regex patterns to parse PostgreSQL, MySQL, and SQLite error formats.
func extractConstraintInfo(qe *QueryError, originalErrMsg string) {
	// Try to extract constraint name
	// PostgreSQL: constraint "constraint_name"
	if idx := strings.Index(originalErrMsg, "constraint \""); idx != -1 {
		start := idx + len("constraint \"")
		if end := strings.Index(originalErrMsg[start:], "\""); end != -1 {
			qe.Constraint = originalErrMsg[start : start+end]
		}
	}

	// MySQL: 'constraint_name'
	if idx := strings.Index(originalErrMsg, "CONSTRAINT `"); idx != -1 {
		start := idx + len("CONSTRAINT `")
		if end := strings.Index(originalErrMsg[start:], "`"); end != -1 {
			qe.Constraint = originalErrMsg[start : start+end]
		}
	}

	// Try to extract table name
	// PostgreSQL: table "table_name"
	if idx := strings.Index(originalErrMsg, "table \""); idx != -1 {
		start := idx + len("table \"")
		if end := strings.Index(originalErrMsg[start:], "\""); end != -1 {
			qe.Table = originalErrMsg[start : start+end]
		}
	}

	// MySQL: table 'table_name' or table `table_name`
	if idx := strings.Index(originalErrMsg, "table `"); idx != -1 {
		start := idx + len("table `")
		if end := strings.Index(originalErrMsg[start:], "`"); end != -1 {
			qe.Table = originalErrMsg[start : start+end]
		}
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

// IsNotFound checks if the error is ErrRecordNotFound.
// It uses errors.Is to check the error chain.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrRecordNotFound) || errors.Is(err, sql.ErrNoRows)
}

// IsConstraintViolation checks if the error is any type of constraint violation.
// This includes unique, foreign key, not null, and check constraints.
func IsConstraintViolation(err error) bool {
	return errors.Is(err, ErrDuplicateKey) ||
		errors.Is(err, ErrForeignKey) ||
		errors.Is(err, ErrNotNullViolation) ||
		errors.Is(err, ErrCheckViolation)
}

// IsDuplicateKey checks if the error is a duplicate key violation.
func IsDuplicateKey(err error) bool {
	return errors.Is(err, ErrDuplicateKey)
}

// IsForeignKeyViolation checks if the error is a foreign key violation.
func IsForeignKeyViolation(err error) bool {
	return errors.Is(err, ErrForeignKey)
}

// IsNotNullViolation checks if the error is a NOT NULL constraint violation.
func IsNotNullViolation(err error) bool {
	return errors.Is(err, ErrNotNullViolation)
}

// IsCheckViolation checks if the error is a CHECK constraint violation.
func IsCheckViolation(err error) bool {
	return errors.Is(err, ErrCheckViolation)
}

// IsDeadlock checks if the error is a transaction deadlock.
func IsDeadlock(err error) bool {
	return errors.Is(err, ErrTransactionDeadlock)
}

// IsSerializationFailure checks if the error is a serialization failure.
func IsSerializationFailure(err error) bool {
	return errors.Is(err, ErrSerializationFailure)
}

// IsConnectionError checks if the error is a connection failure.
// This includes both connection refused and connection lost errors.
func IsConnectionError(err error) bool {
	return errors.Is(err, ErrConnectionFailed) ||
		errors.Is(err, ErrConnectionLost)
}

// IsTimeout checks if the error is a timeout error.
func IsTimeout(err error) bool {
	return errors.Is(err, ErrTimeout)
}

// IsSchemaError checks if the error is a schema-related error.
// This includes missing columns, missing tables, and syntax errors.
func IsSchemaError(err error) bool {
	return errors.Is(err, ErrColumnNotFound) ||
		errors.Is(err, ErrTableNotFound) ||
		errors.Is(err, ErrInvalidSyntax)
}

// GetQueryError extracts the underlying QueryError from an error if present.
// Returns nil if the error is not or does not wrap a QueryError.
// Use this to access query details like the SQL, args, table, and constraint.
func GetQueryError(err error) *QueryError {
	var qe *QueryError
	if errors.As(err, &qe) {
		return qe
	}
	return nil
}

// formatArgs formats query arguments for error messages.
// It limits the output length to avoid excessively long error messages.
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
