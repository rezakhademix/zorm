package zorm

import (
	"context"
	"database/sql/driver"
	"errors"
	"math"
	"testing"
)

func TestWriteGuards_NilEntitiesAndVersionOverflow(t *testing.T) {
	ctx := context.Background()
	db := openFailureDB(t, &failureDB{execErr: errDatabaseFailure, queryErr: errDatabaseFailure})
	m := New[ExModel]().SetDB(db)
	if err := m.Save(ctx, nil); !errors.Is(err, ErrNilPointer) {
		t.Errorf("nil Save should be rejected: %v", err)
	}
	if err := m.UpdateColumns(ctx, nil, "name"); !errors.Is(err, ErrNilPointer) {
		t.Errorf("nil UpdateColumns should be rejected: %v", err)
	}
	if err := m.BulkInsert(ctx, []*ExModel{nil}); !errors.Is(err, ErrNilPointer) {
		t.Errorf("nil batch entity should be rejected: %v", err)
	}
	if _, err := m.Exec(ctx); !errors.Is(err, ErrRequiresRawQuery) {
		t.Errorf("Exec without SQL should be rejected: %v", err)
	}
	versioned := New[saveVersionedItem]().SetDB(db)
	entity := &saveVersionedItem{ID: 1, Name: "before", Version: math.MaxInt64}
	trackOriginals(entity, versioned.modelInfo)
	defer ClearOriginals(entity)
	entity.Name = "after"
	if err := versioned.Save(ctx, entity); !errors.Is(err, ErrVersionOverflow) {
		t.Errorf("overflow should be rejected before database access: %v", err)
	}
	if entity.Version != math.MaxInt64 || GetOriginal(entity, "name") != "before" {
		t.Error("overflow guard changed version or baseline")
	}
}

func TestWriteGuards_ExcludingAllColumnsDoesNotBypassValidation(t *testing.T) {
	ctx := context.Background()
	m := New[saveItem]().Exclude("name", "value")
	if err := m.Update(ctx, nil); !errors.Is(err, ErrNilPointer) {
		t.Errorf("nil Update should be rejected: %v", err)
	}
	if err := m.Save(ctx, nil); !errors.Is(err, ErrNilPointer) {
		t.Errorf("nil Save should be rejected: %v", err)
	}
	entity := &saveItem{ID: 1, Name: "untracked"}
	if err := m.Save(ctx, entity); !errors.Is(err, ErrSaveUntracked) {
		t.Errorf("untracked Save should be rejected: %v", err)
	}
	invalid := m.Clone().Where("invalid;column", 1)
	if err := invalid.Update(ctx, entity); !errors.Is(err, ErrInvalidColumnName) {
		t.Errorf("invalid Update should be rejected: %v", err)
	}
	if err := invalid.Save(ctx, entity); !errors.Is(err, ErrInvalidColumnName) {
		t.Errorf("invalid Save should be rejected: %v", err)
	}
}

func TestPluckFailures_InvalidColumnsAndMalformedResult(t *testing.T) {
	ctx := context.Background()
	db := openFailureDB(t, &failureDB{columns: []string{"id", "value"}, values: [][]driver.Value{{int64(1), int64(2)}}})
	m := New[ExModel]().SetDB(db)
	if _, err := m.Pluck(ctx, "name;drop"); !errors.Is(err, ErrInvalidColumnName) {
		t.Errorf("unsafe pluck column accepted: %v", err)
	}
	if _, err := m.Pluck(ctx, "id"); err == nil || GetQueryError(err) == nil || GetQueryError(err).Operation != "SCAN" {
		t.Errorf("malformed driver result should return SCAN context: %v", err)
	}
}
