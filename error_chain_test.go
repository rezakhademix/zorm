package zorm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/mattn/go-sqlite3"
)

type chainDriverError struct {
	state   string
	message string
}

func (e *chainDriverError) Error() string    { return e.message }
func (e *chainDriverError) SQLState() string { return e.state }

func TestErrorChain_AllClassifications(t *testing.T) {
	tests := []struct {
		name     string
		state    string
		message  string
		sentinel error
	}{
		{"pg unique", "23505", "database error", ErrDuplicateKey},
		{"pg foreign key", "23503", "database error", ErrForeignKey},
		{"pg not null", "23502", "database error", ErrNotNullViolation},
		{"pg check", "23514", "database error", ErrCheckViolation},
		{"pg deadlock", "40P01", "database error", ErrTransactionDeadlock},
		{"pg serialization", "40001", "database error", ErrSerializationFailure},
		{"pg connection failure", "08006", "database error", ErrConnectionFailed},
		{"pg missing connection", "08003", "database error", ErrConnectionFailed},
		{"pg missing column", "42703", "database error", ErrColumnNotFound},
		{"pg missing table", "42P01", "database error", ErrTableNotFound},
		{"pg syntax", "42601", "database error", ErrInvalidSyntax},
		{"pg constraint class", "23P01", "database error", ErrDuplicateKey},
		{"pg connection class", "08004", "database error", ErrConnectionFailed},
		{"pg transaction class", "40003", "database error", ErrTransactionDeadlock},
		{"pg syntax class", "42501", "database error", ErrInvalidSyntax},
		{"pg unknown", "XX000", "database error", nil},
		{"pg short state", "X", "database error", nil},
		{"message unique", "", "UNIQUE constraint failed: users.email", ErrDuplicateKey},
		{"message foreign key", "", "FOREIGN KEY constraint failed", ErrForeignKey},
		{"message not null", "", "NOT NULL constraint failed: users.name", ErrNotNullViolation},
		{"message check", "", "CHECK constraint failed: positive_balance", ErrCheckViolation},
		{"message deadlock", "", "deadlock detected", ErrTransactionDeadlock},
		{"message serialization", "", "could not serialize access", ErrSerializationFailure},
		{"message connection failure", "", "connection refused", ErrConnectionFailed},
		{"message connection lost", "", "connection reset by peer", ErrConnectionLost},
		{"message timeout", "", "query timed out", ErrTimeout},
		{"message column", "", "no such column: missing", ErrColumnNotFound},
		{"message table", "", "no such table: missing", ErrTableNotFound},
		{"message syntax", "", "syntax error near SELECT", ErrInvalidSyntax},
		{"message unknown", "", "database error", nil},
		{"SQLSTATE takes precedence", "23503", "duplicate key", ErrForeignKey},
	}
	for _, tt := range tests {
		for _, chain := range []string{"direct", "wrapped", "joined"} {
			t.Run(tt.name+"/"+chain, func(t *testing.T) {
				original := &chainDriverError{state: tt.state, message: tt.message}
				var input error = original
				marker := errors.New("transport marker")
				switch chain {
				case "wrapped":
					input = fmt.Errorf("transport: %w", fmt.Errorf("driver: %w", original))
				case "joined":
					input = errors.Join(marker, fmt.Errorf("driver: %w", original))
				}
				args := []any{1, "a@x"}
				err := WrapQueryError("INSERT", "INSERT INTO users VALUES (?, ?)", args, input)
				if tt.sentinel != nil && !errors.Is(err, tt.sentinel) {
					t.Errorf("classification lost: want %v, got %v", tt.sentinel, err)
				}
				if !errors.Is(err, input) || !errors.Is(err, original) {
					t.Error("original error identity or wrapper chain lost")
				}
				if chain == "joined" && !errors.Is(err, marker) {
					t.Error("joined error branch lost")
				}
				var driverErr *chainDriverError
				if !errors.As(err, &driverErr) || driverErr != original {
					t.Error("typed driver error lost")
				}
				var stateErr interface{ SQLState() string }
				if !errors.As(err, &stateErr) || stateErr.SQLState() != tt.state {
					t.Error("driver SQLSTATE interface lost")
				}
				qe := GetQueryError(err)
				if qe == nil || qe.Query != "INSERT INTO users VALUES (?, ?)" || qe.Operation != "INSERT" || !reflect.DeepEqual(qe.Args, args) {
					t.Fatalf("query context lost: %+v", qe)
				}
				if !strings.Contains(qe.Error(), input.Error()) {
					t.Error("original error text lost")
				}
				wantMessage := input.Error()
				if tt.sentinel != nil {
					wantMessage = tt.sentinel.Error() + ": " + wantMessage
				}
				if qe.Err.Error() != wantMessage {
					t.Errorf("error formatting changed: got %q, want %q", qe.Err.Error(), wantMessage)
				}
			})
		}
	}
}

func TestErrorChain_SpecialCases(t *testing.T) {
	if err := WrapQueryError("SELECT", "SELECT 1", nil, nil); err != nil {
		t.Errorf("nil error became %v", err)
	}
	if err := WrapQueryError("SELECT", "SELECT 1", nil, fmt.Errorf("driver: %w", sql.ErrNoRows)); err != ErrRecordNotFound {
		t.Errorf("not-found compatibility changed: %v", err)
	}
	for _, original := range []error{context.Canceled, context.DeadlineExceeded} {
		err := WrapQueryError("SELECT", "SELECT 1", nil, original)
		if !errors.Is(err, original) {
			t.Errorf("context error identity lost: %v", err)
		}
		if original == context.DeadlineExceeded && !errors.Is(err, ErrTimeout) {
			t.Error("deadline timeout classification lost")
		}
	}
}

func TestErrorChain_RealSQLiteDriverError(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	_, original := db.Exec(`INSERT INTO save_items (id, name, value) VALUES (1, 'duplicate', 0)`)
	if original == nil {
		t.Fatal("expected a real uniqueness violation")
	}
	err := WrapQueryError("INSERT", "INSERT INTO save_items ...", nil, original)
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatal("SQLite duplicate classification lost")
	}
	var driverErr sqlite3.Error
	if !errors.As(err, &driverErr) || driverErr.ExtendedCode != sqlite3.ErrConstraintPrimaryKey {
		t.Errorf("SQLite extended error code lost: %+v", driverErr)
	}
	if !errors.Is(err, original) {
		t.Error("SQLite original error identity lost")
	}
}
