package zorm

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestErrorChain_ClassifiedPostgresPreservesDriver(t *testing.T) {
	original := &pgconn.PgError{Code: "23505", Message: "duplicate key", ConstraintName: "users_email_key"}
	driverErr := fmt.Errorf("driver: %w", original)
	err := WrapQueryError("INSERT", "INSERT INTO users (email) VALUES ($1)", []any{"a@x"}, driverErr)
	if !errors.Is(err, ErrDuplicateKey) {
		t.Error("classified sentinel was lost")
	}
	if !errors.Is(err, driverErr) || !errors.Is(err, original) {
		t.Error("original error chain was lost")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr != original {
		t.Error("errors.As must recover the original PostgreSQL error")
	}
}
