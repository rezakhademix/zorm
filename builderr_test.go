package zorm

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// The builder had two error-reporting regimes: some methods set buildErr on
// invalid input, others silently dropped the clause and ran the query anyway —
// so a rejected ORDER BY, GROUP BY, HAVING, LOCK, CTE or full-text predicate
// changed what the query meant without telling anyone. These tests pin the
// single regime: rejected input sets buildErr and surfaces on the terminal call.

// TestModelBuilder_InvalidInputSetsBuildErr covers the Model builder methods
// that used to skip invalid input silently.
func TestModelBuilder_InvalidInputSetsBuildErr(t *testing.T) {
	const badColumn = "name; DROP TABLE users"

	cases := []struct {
		name  string
		build func() *Model[TestModel]
	}{
		{"OrderBy column", func() *Model[TestModel] { return New[TestModel]().OrderBy(badColumn, "ASC") }},
		{"OrderBy direction", func() *Model[TestModel] { return New[TestModel]().OrderBy("name", "DROP") }},
		{"GroupBy", func() *Model[TestModel] { return New[TestModel]().GroupBy("status", badColumn) }},
		{"GroupByRollup", func() *Model[TestModel] { return New[TestModel]().GroupByRollup("status", badColumn) }},
		{"GroupByCube", func() *Model[TestModel] { return New[TestModel]().GroupByCube("status", badColumn) }},
		{"GroupByGroupingSets", func() *Model[TestModel] {
			return New[TestModel]().GroupByGroupingSets([]string{"status"}, []string{badColumn})
		}},
		{"Having", func() *Model[TestModel] { return New[TestModel]().Having("COUNT(*) > 1; DROP TABLE users") }},
		{"WithCTE", func() *Model[TestModel] { return New[TestModel]().WithCTE(badColumn, "SELECT 1") }},
		{"Lock", func() *Model[TestModel] { return New[TestModel]().Lock("UPDATE; DROP TABLE users") }},
		{"WhereFullText", func() *Model[TestModel] { return New[TestModel]().WhereFullText(badColumn, "x") }},
		{"WhereFullTextWithConfig column", func() *Model[TestModel] {
			return New[TestModel]().WhereFullTextWithConfig(badColumn, "x", "english")
		}},
		{"WhereFullTextWithConfig config", func() *Model[TestModel] {
			return New[TestModel]().WhereFullTextWithConfig("content", "x", "english'; DROP TABLE users")
		}},
		{"WhereTsVector", func() *Model[TestModel] { return New[TestModel]().WhereTsVector(badColumn, "x") }},
		{"WherePhraseSearch", func() *Model[TestModel] { return New[TestModel]().WherePhraseSearch(badColumn, "x") }},
	}

	ctx := context.Background()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := tc.build()
			if m.buildErr == nil {
				t.Fatal("expected buildErr to be set")
			}

			// And it must reach the caller instead of running a query that
			// silently means something else.
			if _, err := m.Get(ctx); !errors.Is(err, ErrInvalidColumnName) {
				t.Errorf("expected ErrInvalidColumnName from Get, got %v", err)
			}

			query, _ := m.Print()
			if strings.Contains(query, "DROP") {
				t.Errorf("rejected input leaked into SQL: %q", query)
			}
		})
	}
}

// TestModelBuilder_ValidInputUnaffected guards against overcorrecting.
func TestModelBuilder_ValidInputUnaffected(t *testing.T) {
	m := New[TestModel]().
		OrderBy("name", "asc").
		GroupBy("status").
		Having("COUNT(*) > ?", 1).
		WithCTE("recent", "SELECT 1").
		Lock("UPDATE").
		WhereFullText("content", "term")

	if m.buildErr != nil {
		t.Fatalf("valid builder chain set buildErr: %v", m.buildErr)
	}
	query, _ := m.Print()
	for _, want := range []string{"ORDER BY name ASC", "GROUP BY status", "HAVING", "WITH recent AS", "FOR UPDATE"} {
		if !strings.Contains(query, want) {
			t.Errorf("expected %q in %q", want, query)
		}
	}
}

// TestScalarBuilder_InvalidInputSetsBuildErr covers the same regime for
// ScalarQuery, which skipped invalid column names everywhere.
func TestScalarBuilder_InvalidInputSetsBuildErr(t *testing.T) {
	const badColumn = "name; DROP TABLE users"

	cases := []struct {
		name  string
		build func() *ScalarQuery[string]
	}{
		{"Where column", func() *ScalarQuery[string] { return Query[string]().Where(badColumn, 1) }},
		{"Where operator column", func() *ScalarQuery[string] { return Query[string]().Where(badColumn, ">", 1) }},
		{"Where map", func() *ScalarQuery[string] {
			return Query[string]().Where(map[string]any{badColumn: 1})
		}},
		{"Where non-string", func() *ScalarQuery[string] { return Query[string]().Where(42, 1) }},
		{"WhereNull", func() *ScalarQuery[string] { return Query[string]().WhereNull(badColumn) }},
		{"WhereNotNull", func() *ScalarQuery[string] { return Query[string]().WhereNotNull(badColumn) }},
		{"OrderBy column", func() *ScalarQuery[string] { return Query[string]().OrderBy(badColumn, "ASC") }},
		{"OrderBy direction", func() *ScalarQuery[string] { return Query[string]().OrderBy("id", "DROP") }},
		{"GroupBy", func() *ScalarQuery[string] { return Query[string]().GroupBy("status", badColumn) }},
		{"Having", func() *ScalarQuery[string] { return Query[string]().Having("COUNT(*) > 1; DROP TABLE users") }},
	}

	db := setupScalarTestDB(t)
	defer db.Close()
	ctx := context.Background()

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := tc.build()
			if q.buildErr == nil {
				t.Fatal("expected buildErr to be set")
			}
			if _, err := q.SetDB(db).Table("users").Select("name").Get(ctx); err == nil {
				t.Error("expected the build error to surface from Get")
			}
		})
	}
}

// TestScalarBuilder_ValidInputUnaffected guards against overcorrecting.
func TestScalarBuilder_ValidInputUnaffected(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()

	names, err := Query[string]().
		SetDB(db).
		Table("users").
		Select("name").
		Where("id", ">", 0).
		WhereNotNull("name").
		OrderBy("id", "asc").
		Get(context.Background())
	if err != nil {
		t.Fatalf("valid scalar chain failed: %v", err)
	}
	if len(names) == 0 {
		t.Error("expected rows back from a valid scalar query")
	}
}
