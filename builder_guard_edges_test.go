package zorm

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type emptyConstraintMethods struct{}

func (emptyConstraintMethods) GetWheres() {}
func (emptyConstraintMethods) GetArgs()   {}

type wrongConstraintMethods struct{}

func (wrongConstraintMethods) GetWheres() string { return "ignored" }
func (wrongConstraintMethods) GetArgs() []string { return []string{"ignored"} }

type unsafeStructFilter struct {
	Name string `zorm:"column:bad-name"`
}

func TestBuilderGuards_ConstraintExtractionAndStructFilters(t *testing.T) {
	for _, input := range []any{nil, 1, emptyConstraintMethods{}, wrongConstraintMethods{}} {
		wheres, args, err := extractQueryConstraints(input)
		if err != nil || len(wheres) != 0 || len(args) != 0 {
			t.Errorf("invalid constraint source was accepted: %T", input)
		}
	}
	for _, input := range []any{&ExModel{Name: "A"}, struct{ Name string }{Name: "A"}} {
		m := New[ExModel]().Where(input)
		if m.buildErr != nil || len(m.wheres) != 1 || len(m.args) != 1 || m.args[0] != "A" {
			t.Errorf("struct filter did not bind nonzero value: %T, %v %v", input, m.wheres, m.args)
		}
	}
	if m := New[ExModel]().Where(unsafeStructFilter{Name: "A"}); !errors.Is(m.buildErr, ErrInvalidColumnName) {
		t.Errorf("unsafe explicit column tag accepted: %v", m.buildErr)
	}
	if m := New[ExModel]().addWhere("AND", "name;drop", "A"); !errors.Is(m.buildErr, ErrInvalidColumnName) {
		t.Errorf("unsafe internal column form accepted: %v", m.buildErr)
	}
	for _, source := range []string{"name IS NOT", "name IS", "name >="} {
		if _, err := buildWhereComparison(source); err != nil {
			t.Errorf("supported trailing operator rejected: %s", source)
		}
	}
	for _, values := range [][]reflect.Value{nil, {reflect.ValueOf(42)}} {
		m := New[ExModel]().Where("id", 1)
		m.applyWhereHas(values, nil)
		if len(m.wheres) != 1 {
			t.Error("invalid relation result changed existing predicates")
		}
	}
}

func TestBuilderGuards_DeferredErrorsReachEveryWrite(t *testing.T) {
	ctx := context.Background()
	m := New[ExModel]().Select("id;drop")
	for _, run := range []func() error{
		func() error { return m.Delete(ctx) },
		func() error { return m.ForceDeleteAll(ctx) },
		func() error { return m.UpdateMany(ctx, map[string]any{"name": "x"}) },
		func() error { _, err := m.Cursor(ctx); return err },
	} {
		if err := run(); err != m.buildErr {
			t.Errorf("deferred error was lost: %v", err)
		}
	}
	if err := New[ExModel]().Having("COUNT(*)", 1).buildErr; err != nil {
		t.Fatal(err)
	}
	if got, _ := New[ExModel]().Oldest().Print(); !strings.Contains(got, "created_at ASC") {
		t.Error(got)
	}
	// Unsupported condition values have historically been ignored by the model
	// builder; keep that behavior explicit while checking its no-predicate path.
	if m := New[ExModel]().Where(42); len(m.wheres) != 0 {
		t.Error("unsupported condition unexpectedly added a predicate")
	}
	db := setupExDB(t)
	defer db.Close()
	if err := New[ExModel]().SetDB(db).Where("id", 999).Chunk(ctx, 10, func([]*ExModel) error { t.Error("callback on empty data"); return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestScalarGuards_DefaultColumnAndGlobalFallback(t *testing.T) {
	oldAtomic, oldLegacy, oldResolver := globalDB.Load(), GlobalDB, GetGlobalResolver()
	defer func() { globalDB.Store(oldAtomic); GlobalDB = oldLegacy; globalResolver.Store(oldResolver) }()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE scalar_default (value INTEGER); INSERT INTO scalar_default VALUES (3)`); err != nil {
		t.Fatal(err)
	}
	ClearDBResolver()
	SetGlobalDB(db)
	q := Query[int]().Table("scalar_default").SetDB(nil)
	values, err := q.Get(context.Background())
	if err != nil || len(values) != 1 || values[0] != 3 {
		t.Fatalf("default SELECT or global DB fallback failed: %v %v", values, err)
	}
}
