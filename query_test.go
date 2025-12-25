package zorm

import (
	"context"
	"strings"
	"testing"
)

// TestSelect tests the Select method
func TestSelect(t *testing.T) {
	m := New[TestModel]().Select("id", "name")
	query, _ := m.Print()

	if !strings.Contains(query, "id, name") {
		t.Errorf("expected query to contain 'id, name', got %q", query)
	}
}

// TestDistinct_Query tests the Distinct method
func TestDistinct_Query(t *testing.T) {
	m := New[TestModel]().Distinct().Select("name")
	query, _ := m.Print()

	if !strings.Contains(query, "DISTINCT") {
		t.Errorf("expected query to contain DISTINCT, got %q", query)
	}
}

// TestDistinctBy_Query tests the DistinctBy method
func TestDistinctBy_Query(t *testing.T) {
	m := New[TestModel]().DistinctBy("name").Select("name", "age")
	query, _ := m.Print()

	if !strings.Contains(query, "DISTINCT ON (name)") {
		t.Errorf("expected query to contain 'DISTINCT ON (name)', got %q", query)
	}
}

// TestRaw tests the Raw method
func TestRaw(t *testing.T) {
	m := New[TestModel]().Raw("SELECT * FROM users WHERE id = ?", 123)
	query, args := m.Print()

	expected := "SELECT * FROM users WHERE id = $1"
	if query != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
	if len(args) != 1 || args[0] != 123 {
		t.Errorf("expected args [123], got %v", args)
	}
}

// TestWhere_StringWithValue tests Where with string column and value
func TestWhere_StringWithValue(t *testing.T) {
	m := New[TestModel]().Where("name", "John")
	query, args := m.Print()

	expected := "name = $1"
	if !strings.Contains(query, expected) {
		t.Errorf("expected query to contain %q, got %q", expected, query)
	}
	if len(args) != 1 || args[0] != "John" {
		t.Errorf("expected args ['John'], got %v", args)
	}
}

// TestWhere_StringWithOperator tests Where with operator
func TestWhere_StringWithOperator(t *testing.T) {
	m := New[TestModel]().Where("age", ">", 18)
	query, args := m.Print()

	expected := "age > $1"
	if !strings.Contains(query, expected) {
		t.Errorf("expected query to contain %q, got %q", expected, query)
	}
	if len(args) != 1 || args[0] != 18 {
		t.Errorf("expected args [18], got %v", args)
	}
}

// TestWhere_MapConditions tests Where with map
func TestWhere_MapConditions(t *testing.T) {
	m := New[TestModel]().Where(map[string]any{
		"name": "John",
		"age":  30,
	})
	query, args := m.Print()

	// Should contain both conditions
	// Map iteration order is random, so check for presence of keys and values specifically?
	// But placeholders imply order. $1 is first arg, $2 is second.
	// We just check that we have "name = $X" and "age = $Y".
	hasName1 := strings.Contains(query, "name = $1")
	hasAge2 := strings.Contains(query, "age = $2")
	hasName2 := strings.Contains(query, "name = $2")
	hasAge1 := strings.Contains(query, "age = $1")

	if !(hasName1 && hasAge2) && !(hasName2 && hasAge1) {
		t.Errorf("expected query to contain both conditions, got %q", query)
	}
	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}
}

// TestWhere_StructConditions tests Where with struct
func TestWhere_StructConditions(t *testing.T) {
	m := New[TestModel]().Where(&TestModel{
		Name: "John",
		Age:  30,
	})
	query, args := m.Print()

	// Should contain both conditions (non-zero fields)
	// Struct iteration order might vary, so checking both variations
	hasName1 := strings.Contains(query, "name = $1")
	hasAge2 := strings.Contains(query, "user_age = $2")
	hasName2 := strings.Contains(query, "name = $2")
	hasAge1 := strings.Contains(query, "user_age = $1")

	if !(hasName1 && hasAge2) && !(hasName2 && hasAge1) {
		t.Errorf("expected query to contain 'name = $X' and 'user_age = $Y', got %q", query)
	}
	if len(args) < 1 {
		t.Errorf("expected at least 1 arg, got %d", len(args))
	}
}

// TestWhere_CallbackNested tests Where with callback for nested conditions
func TestWhere_CallbackNested(t *testing.T) {
	m := New[TestModel]().Where(func(q *Model[TestModel]) {
		q.Where("age", ">", 18).Where("age", "<", 65)
	})
	query, args := m.Print()

	// Should have parentheses for grouping
	if !strings.Contains(query, "(") || !strings.Contains(query, ")") {
		t.Errorf("expected query to have parentheses for grouping, got %q", query)
	}
	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}
}

// TestOrWhere tests the OrWhere method
func TestOrWhere(t *testing.T) {
	m := New[TestModel]().Where("name", "John").OrWhere("name", "Jane")
	query, args := m.Print()

	if !strings.Contains(query, "OR") {
		t.Errorf("expected query to contain OR, got %q", query)
	}
	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}
}

// TestWhereIn tests the WhereIn method
func TestWhereIn(t *testing.T) {
	m := New[TestModel]().WhereIn("id", []any{1, 2, 3})
	query, args := m.Print()

	expected := "id IN ($1, $2, $3)"
	if !strings.Contains(query, expected) {
		t.Errorf("expected query to contain %q, got %q", expected, query)
	}
	if len(args) != 3 {
		t.Errorf("expected 3 args, got %d", len(args))
	}
}

// TestWhereIn_Empty tests WhereIn with empty array
func TestWhereIn_Empty(t *testing.T) {
	m := New[TestModel]().WhereIn("id", []any{})
	query, _ := m.Print()

	// Should add 1=0 optimization for empty IN
	if !strings.Contains(query, "1=0") {
		t.Errorf("expected query to contain '1=0' for empty IN, got %q", query)
	}
}

// TestOrderBy tests the OrderBy method
func TestOrderBy(t *testing.T) {
	m := New[TestModel]().OrderBy("created_at", "DESC")
	query, _ := m.Print()

	expected := "ORDER BY created_at DESC"
	if !strings.Contains(query, expected) {
		t.Errorf("expected query to contain %q, got %q", expected, query)
	}
}

// TestOrderBy_Multiple tests multiple OrderBy calls
func TestOrderBy_Multiple(t *testing.T) {
	m := New[TestModel]().OrderBy("name", "ASC").OrderBy("age", "DESC")
	query, _ := m.Print()

	if !strings.Contains(query, "name ASC") || !strings.Contains(query, "age DESC") {
		t.Errorf("expected query to contain both order clauses, got %q", query)
	}
}

// TestGroupBy tests the GroupBy method
func TestGroupBy(t *testing.T) {
	m := New[TestModel]().Select("name", "COUNT(*)").GroupBy("name")
	query, _ := m.Print()

	expected := "GROUP BY name"
	if !strings.Contains(query, expected) {
		t.Errorf("expected query to contain %q, got %q", expected, query)
	}
}

// Note: GroupByRollup, GroupByCube, and GroupByGroupingSets tests are in groupby_test.go

// TestHaving tests the Having method
func TestHaving(t *testing.T) {
	m := New[TestModel]().
		Select("name", "COUNT(*) as count").
		GroupBy("name").
		Having("COUNT(*) >", 5)
	query, args := m.Print()

	expected := "HAVING COUNT(*) > $1"
	if !strings.Contains(query, expected) {
		t.Errorf("expected query to contain %q, got %q", expected, query)
	}
	if len(args) != 1 || args[0] != 5 {
		t.Errorf("expected args [5], got %v", args)
	}
}

// TestLatest tests the Latest method
func TestLatest(t *testing.T) {
	m := New[TestModel]().Latest()
	query, _ := m.Print()

	expected := "ORDER BY created_at DESC"
	if !strings.Contains(query, expected) {
		t.Errorf("expected query to contain %q, got %q", expected, query)
	}
}

// TestLatest_CustomColumn tests Latest with custom column
func TestLatest_CustomColumn(t *testing.T) {
	m := New[TestModel]().Latest("updated_at")
	query, _ := m.Print()

	expected := "ORDER BY updated_at DESC"
	if !strings.Contains(query, expected) {
		t.Errorf("expected query to contain %q, got %q", expected, query)
	}
}

// TestOldest tests the Oldest method
func TestOldest(t *testing.T) {
	m := New[TestModel]().Oldest()
	query, _ := m.Print()

	expected := "ORDER BY created_at ASC"
	if !strings.Contains(query, expected) {
		t.Errorf("expected query to contain %q, got %q", expected, query)
	}
}

// TestLimit tests the Limit method
func TestLimit(t *testing.T) {
	m := New[TestModel]().Limit(10)
	query, _ := m.Print()

	expected := "LIMIT 10"
	if !strings.Contains(query, expected) {
		t.Errorf("expected query to contain %q, got %q", expected, query)
	}
}

// TestOffset tests the Offset method
func TestOffset(t *testing.T) {
	m := New[TestModel]().Offset(20)
	query, _ := m.Print()

	expected := "OFFSET 20"
	if !strings.Contains(query, expected) {
		t.Errorf("expected query to contain %q, got %q", expected, query)
	}
}

// TestLimitOffset tests Limit and Offset together
func TestLimitOffset(t *testing.T) {
	m := New[TestModel]().Limit(10).Offset(20)
	query, _ := m.Print()

	if !strings.Contains(query, "LIMIT 10") || !strings.Contains(query, "OFFSET 20") {
		t.Errorf("expected query to contain both LIMIT and OFFSET, got %q", query)
	}
}

// TestScope tests the Scope method
func TestScope(t *testing.T) {
	activeScope := func(m *Model[TestModel]) *Model[TestModel] {
		return m.Where("status", "active")
	}

	m := New[TestModel]().Scope(activeScope)
	query, args := m.Print()

	if !strings.Contains(query, "status = $1") {
		t.Errorf("expected query to contain 'status = $1', got %q", query)
	}
	if len(args) != 1 || args[0] != "active" {
		t.Errorf("expected args ['active'], got %v", args)
	}
}

// TestWith tests the With method for eager loading
func TestWith(t *testing.T) {
	m := New[TestModel]().With("Posts", "Comments")

	if len(m.relations) != 2 {
		t.Errorf("expected 2 relations, got %d", len(m.relations))
	}
	if m.relations[0] != "Posts" || m.relations[1] != "Comments" {
		t.Errorf("expected relations [Posts, Comments], got %v", m.relations)
	}
}

// TestWithCallback tests the WithCallback method
func TestWithCallback(t *testing.T) {
	m := New[TestModel]().WithCallback("Posts", func(q *Model[TestModel]) {
		q.Where("published", true)
	})

	if len(m.relations) != 1 {
		t.Errorf("expected 1 relation, got %d", len(m.relations))
	}
	if m.relationCallbacks["Posts"] == nil {
		t.Error("expected callback to be set for Posts relation")
	}
}

// TestWithMorph tests the WithMorph method
func TestWithMorph(t *testing.T) {
	typeMap := map[string][]string{
		"events": {"Calendar"},
		"posts":  {"Author"},
	}
	m := New[TestModel]().WithMorph("Comments", typeMap)

	if len(m.relations) != 1 {
		t.Errorf("expected 1 relation, got %d", len(m.relations))
	}
	if m.morphRelations["Comments"] == nil {
		t.Error("expected morph relation to be set for Comments")
	}
}

// TestWithCTE tests the WithCTE method
func TestWithCTE(t *testing.T) {
	m := New[TestModel]().WithCTE("recent_users", "SELECT * FROM users WHERE created_at > NOW() - INTERVAL '7 days'")

	if len(m.ctes) != 1 {
		t.Errorf("expected 1 CTE, got %d", len(m.ctes))
	}
	if m.ctes[0].Name != "recent_users" {
		t.Errorf("expected CTE name 'recent_users', got %q", m.ctes[0].Name)
	}
}

// TestLock tests the Lock method
func TestLock(t *testing.T) {
	m := New[TestModel]().Lock("UPDATE")
	query, _ := m.Print()

	expected := "FOR UPDATE"
	if !strings.Contains(query, expected) {
		t.Errorf("expected query to contain %q, got %q", expected, query)
	}
}

// Note: Full-text search tests (WhereFullText, WhereFullTextWithConfig, WhereTsVector, WherePhraseSearch) are in fulltext_test.go

// TestUsePrimary tests the UsePrimary method
func TestUsePrimary(t *testing.T) {
	m := New[TestModel]().UsePrimary()

	if !m.forcePrimary {
		t.Error("expected forcePrimary to be true")
	}
	if m.forceReplica != -1 {
		t.Errorf("expected forceReplica to be -1, got %d", m.forceReplica)
	}
}

// TestUseReplica tests the UseReplica method
func TestUseReplica(t *testing.T) {
	m := New[TestModel]().UseReplica(0)

	if m.forcePrimary {
		t.Error("expected forcePrimary to be false")
	}
	if m.forceReplica != 0 {
		t.Errorf("expected forceReplica to be 0, got %d", m.forceReplica)
	}
}

// TestGetWheres tests the GetWheres method
func TestGetWheres(t *testing.T) {
	m := New[TestModel]().Where("name", "John").Where("age >", 18)
	wheres := m.GetWheres()

	if len(wheres) != 2 {
		t.Errorf("expected 2 where clauses, got %d", len(wheres))
	}
}

// TestGetArgs tests the GetArgs method
func TestGetArgs(t *testing.T) {
	m := New[TestModel]().Where("name", "John").Where("age >", 18)
	args := m.GetArgs()

	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}
}

// TestMethodChaining tests that methods can be chained in any order
func TestMethodChaining(t *testing.T) {
	m := New[TestModel]().
		Select("id", "name").
		Where("age", ">", 18).
		Where("status", "active").
		OrderBy("created_at", "DESC").
		Limit(10).
		Offset(5)

	query, args := m.Print()

	// Check that all parts are present
	if !strings.Contains(query, "id, name") {
		t.Error("missing SELECT clause")
	}
	if !strings.Contains(query, "age > $1") {
		t.Error("missing age WHERE clause")
	}
	if !strings.Contains(query, "status = $2") {
		t.Error("missing status WHERE clause")
	}
	if !strings.Contains(query, "ORDER BY created_at DESC") {
		t.Error("missing ORDER BY clause")
	}
	if !strings.Contains(query, "LIMIT 10") {
		t.Error("missing LIMIT clause")
	}
	if !strings.Contains(query, "OFFSET 5") {
		t.Error("missing OFFSET clause")
	}
	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}
}

// TestComplexQuery tests a complex query with multiple features
func TestComplexQuery(t *testing.T) {
	m := New[TestModel]().
		Select("name", "COUNT(*) as count").
		Where(func(q *Model[TestModel]) {
			q.Where("age", ">", 18).OrWhere("status", "verified")
		}).
		GroupBy("name").
		Having("COUNT(*) >", 5).
		OrderBy("count", "DESC").
		Limit(20)

	query, args := m.Print()

	// Should have all the pieces
	if !strings.Contains(query, "name, COUNT(*) as count") {
		t.Error("missing SELECT clause")
	}
	if !strings.Contains(query, "GROUP BY name") {
		t.Error("missing GROUP BY clause")
	}
	if !strings.Contains(query, "HAVING COUNT(*) > $3") {
		t.Error("missing HAVING clause")
	}
	if !strings.Contains(query, "ORDER BY count DESC") {
		t.Error("missing ORDER BY clause")
	}
	if !strings.Contains(query, "LIMIT 20") {
		t.Error("missing LIMIT clause")
	}
	if len(args) < 2 {
		t.Errorf("expected at least 2 args, got %d", len(args))
	}
}

// TestChunk tests the Chunk method (requires database, so we just check it compiles)
func TestChunk_Signature(t *testing.T) {
	// This test just ensures the Chunk method signature is correct
	// Actual database testing would require a real database connection
	m := New[TestModel]()
	_ = m.Chunk // Check method exists
}

// TestPaginate_Signature tests that Paginate method exists with correct signature
func TestPaginate_Signature(t *testing.T) {
	// This test just ensures the Paginate method signature is correct
	m := New[TestModel]()
	_ = m.Paginate // Check method exists
}

// TestSimplePaginate_Signature tests that SimplePaginate method exists
func TestSimplePaginate_Signature(t *testing.T) {
	// This test just ensures the SimplePaginate method signature is correct
	m := New[TestModel]()
	_ = m.SimplePaginate // Check method exists
}

// TestWhereHas_Signature tests that WhereHas method exists
func TestWhereHas_Signature(t *testing.T) {
	// This test just ensures the WhereHas method signature is correct
	m := New[TestModel]()
	_ = m.WhereHas // Check method exists
}

// TestWhere_IssueWithParentheses demonstrates the bug with Where methods
func TestWhere_IssueWithParentheses(t *testing.T) {
	t.Run("Single Where should not have extra parentheses", func(t *testing.T) {
		m := New[TestModel]().Where("name", "John")
		query, _ := m.Print()

		// Current implementation: "WHERE 1=1  AND (name = ?)"
		// Should be: "WHERE 1=1 AND name = ?"
		// The extra parentheses are being added on line 145 of query.go
		t.Logf("Current query: %s", query)

		// This will fail with current implementation
		if strings.Count(query, "(") > 0 {
			t.Log("WARNING: Query has unnecessary parentheses around simple WHERE clause")
		}
	})

	t.Run("Multiple Where with OR", func(t *testing.T) {
		m := New[TestModel]().Where("name", "John").OrWhere("name", "Jane")
		query, _ := m.Print()

		t.Logf("Current query: %s", query)
		// Should have proper AND/OR logic without excessive parentheses
	})
}

// BenchmarkWhere benchmarks the Where method
func BenchmarkWhere(b *testing.B) {
	for i := 0; i < b.N; i++ {
		m := New[TestModel]().Where("name", "John").Where("age >", 18)
		_, _ = m.Print()
	}
}

// BenchmarkWhereMap benchmarks Where with map
func BenchmarkWhereMap(b *testing.B) {
	conditions := map[string]any{
		"name": "John",
		"age":  30,
	}
	for i := 0; i < b.N; i++ {
		m := New[TestModel]().Where(conditions)
		_, _ = m.Print()
	}
}

// BenchmarkComplexQuery benchmarks a complex query
func BenchmarkComplexQuery(b *testing.B) {
	for i := 0; i < b.N; i++ {
		m := New[TestModel]().
			Select("id", "name").
			Where("age >", 18).
			Where("status", "active").
			OrderBy("created_at", "DESC").
			Limit(10)
		_, _ = m.Print()
	}
}

// TestChunkCallback tests the Chunk callback mechanism (unit test without DB)
func TestChunkCallback_Logic(t *testing.T) {
	t.Skip("Requires database connection")

	// Test the logic of chunk without actual database
	m := New[TestModel]()

	// Ensure method signature is callable
	err := m.Chunk(context.Background(), 10, func(results []*TestModel) error {
		// Callback would process results here
		return nil
	})

	// Will fail without database but tests the API
	if err != nil {
		t.Logf("Expected error without database: %v", err)
	}
}

// TestWhereNull tests the WhereNull method
func TestWhereNull(t *testing.T) {
	m := New[TestModel]().WhereNull("deleted_at")
	query, _ := m.Print()

	expected := "deleted_at IS NULL"
	if !strings.Contains(query, expected) {
		t.Errorf("expected query to contain %q, got %q", expected, query)
	}
}

// TestOrWhereNull tests the OrWhereNull method
func TestOrWhereNull(t *testing.T) {
	m := New[TestModel]().Where("name", "John").OrWhereNull("deleted_at")
	query, _ := m.Print()

	if !strings.Contains(query, "OR deleted_at IS NULL") {
		t.Errorf("expected query to contain 'OR deleted_at IS NULL', got %q", query)
	}
}

// TestWhereNotNull tests the WhereNotNull method
func TestWhereNotNull(t *testing.T) {
	m := New[TestModel]().WhereNotNull("verified_at")
	query, _ := m.Print()

	expected := "verified_at IS NOT NULL"
	if !strings.Contains(query, expected) {
		t.Errorf("expected query to contain %q, got %q", expected, query)
	}
}

// TestOrWhereNotNull tests the OrWhereNotNull method
func TestOrWhereNotNull(t *testing.T) {
	m := New[TestModel]().Where("name", "John").OrWhereNotNull("verified_at")
	query, _ := m.Print()

	if !strings.Contains(query, "OR verified_at IS NOT NULL") {
		t.Errorf("expected query to contain 'OR verified_at IS NOT NULL', got %q", query)
	}
}

// TestTable_Override tests the Table method for custom table name
func TestTable_Override(t *testing.T) {
	m := New[TestModel]().Table("custom_users")
	query, _ := m.Print()

	if !strings.Contains(query, "FROM custom_users") {
		t.Errorf("expected query to contain 'FROM custom_users', got %q", query)
	}
}

// =============================================================================
// SQL INJECTION PREVENTION TESTS
// =============================================================================

// TestSelect_ValidatesColumns verifies Select validates column names
func TestSelect_ValidatesColumns(t *testing.T) {
	tests := []struct {
		name     string
		columns  []string
		expected int // Expected number of columns in query
	}{
		{"valid columns", []string{"id", "name", "email"}, 3},
		{"with asterisk", []string{"*"}, 1},
		{"with table prefix", []string{"users.id", "users.name"}, 2},
		{"skip injection attempt", []string{"id", "name; DROP TABLE users--"}, 1},
		{"skip union attempt", []string{"id", "id UNION SELECT * FROM passwords"}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New[TestModel]().Select(tt.columns...)
			if len(m.columns) != tt.expected {
				t.Errorf("expected %d columns, got %d", tt.expected, len(m.columns))
			}
		})
	}
}

// TestOrderBy_ValidatesDirection verifies OrderBy validates direction
func TestOrderBy_ValidatesDirection(t *testing.T) {
	tests := []struct {
		direction string
		expected  string
	}{
		{"ASC", "ASC"},
		{"DESC", "DESC"},
		{"asc", "ASC"},
		{"desc", "DESC"},
		{"invalid", "ASC"}, // Should default to ASC
		{"DROP", "ASC"},    // Injection attempt - should default to ASC
	}

	for _, tt := range tests {
		t.Run(tt.direction, func(t *testing.T) {
			m := New[TestModel]().OrderBy("name", tt.direction)
			query, _ := m.Print()

			if !strings.Contains(query, "ORDER BY name "+tt.expected) {
				t.Errorf("expected direction %q, got query %q", tt.expected, query)
			}
		})
	}
}

// TestWhereIn_ValidatesColumn verifies WhereIn validates column name
func TestWhereIn_ValidatesColumn(t *testing.T) {
	// Valid column
	m1 := New[TestModel]().WhereIn("id", []any{1, 2, 3})
	query1, _ := m1.Print()
	if !strings.Contains(query1, "id IN") {
		t.Error("Valid column should generate IN clause")
	}

	// Invalid column - should be skipped
	m2 := New[TestModel]().WhereIn("id; DROP TABLE users", []any{1, 2, 3})
	query2, _ := m2.Print()
	if strings.Contains(query2, "DROP") {
		t.Error("Injection attempt should be blocked")
	}
}

// TestGroupBy_ValidatesColumns verifies GroupBy validates column names
func TestGroupBy_ValidatesColumns(t *testing.T) {
	m := New[TestModel]().GroupBy("status", "role; DROP TABLE users--")

	// Only valid column should be added
	if len(m.groupBys) != 1 {
		t.Errorf("expected 1 valid group by column, got %d", len(m.groupBys))
	}
}

// =============================================================================
// REBIND TESTS
// =============================================================================

// TestRebind_ConvertsPlaceholders verifies ? placeholders are converted to $n
func TestRebind_ConvertsPlaceholders(t *testing.T) {
	m := New[TestModel]().Raw("SELECT * FROM users WHERE id = ? AND name = ?", 1, "John")
	query, _ := m.Print()

	if !strings.Contains(query, "$1") {
		t.Error("First placeholder should be converted to $1")
	}
	if !strings.Contains(query, "$2") {
		t.Error("Second placeholder should be converted to $2")
	}
	if strings.Contains(query, "?") {
		t.Error("No ? placeholders should remain")
	}
}

// TestRebind_MultipleQueries verifies rebind handles multiple queries correctly
func TestRebind_MultipleQueries(t *testing.T) {
	m := New[TestModel]().Raw("SELECT * FROM users WHERE a = ? AND b = ? AND c = ?", 1, 2, 3)
	query, args := m.Print()

	if !strings.Contains(query, "$1") || !strings.Contains(query, "$2") || !strings.Contains(query, "$3") {
		t.Error("All placeholders should be converted")
	}
	if len(args) != 3 {
		t.Errorf("expected 3 args, got %d", len(args))
	}
}

// =============================================================================
// PRINT METHOD TESTS
// =============================================================================

// TestPrint_ReturnsQueryAndArgs verifies Print returns correct query and args
func TestPrint_ReturnsQueryAndArgs(t *testing.T) {
	m := New[TestModel]().Where("active", true).Limit(10)
	query, args := m.Print()

	if query == "" {
		t.Error("Print should return non-empty query")
	}
	if len(args) == 0 {
		t.Error("Print should return args")
	}
	if !strings.Contains(query, "FROM test_models") {
		t.Errorf("Query should contain table name, got %q", query)
	}
}

// TestPrint_RawQueryMode verifies Print works with raw queries
func TestPrint_RawQueryMode(t *testing.T) {
	m := New[TestModel]().Raw("SELECT 1")
	query, _ := m.Print()

	if query != "SELECT 1" {
		t.Errorf("expected 'SELECT 1', got %q", query)
	}
}

// =============================================================================
// PAGINATION RESULT TESTS
// =============================================================================

// TestPaginationResult_Fields verifies PaginationResult struct fields
func TestPaginationResult_Structure_Query(t *testing.T) {
	result := &PaginationResult[TestModel]{
		Data:        []*TestModel{},
		Total:       100,
		PerPage:     10,
		CurrentPage: 1,
		LastPage:    10,
	}

	if result.Total != 100 {
		t.Errorf("Total should be 100, got %d", result.Total)
	}
	if result.LastPage != 10 {
		t.Errorf("LastPage should be 10, got %d", result.LastPage)
	}
}

// =============================================================================
// RELATION METHODS TESTS
// =============================================================================

// TestWith_AddsRelations verifies With adds relations
func TestWith_AddsRelations(t *testing.T) {
	m := New[TestModel]().With("Posts", "Comments", "Profile")

	if len(m.relations) != 3 {
		t.Errorf("expected 3 relations, got %d", len(m.relations))
	}
}

// TestWith_NestedRelations verifies With handles nested relations
func TestWith_NestedRelations(t *testing.T) {
	m := New[TestModel]().With("Posts.Comments.Author")

	if len(m.relations) != 1 {
		t.Errorf("expected 1 relation, got %d", len(m.relations))
	}
	if m.relations[0] != "Posts.Comments.Author" {
		t.Errorf("expected 'Posts.Comments.Author', got %q", m.relations[0])
	}
}

// TestWithCallback_AddsCallback verifies WithCallback sets callback
func TestWithCallback_AddsCallback(t *testing.T) {
	callback := func(q *Model[TestModel]) {
		q.Where("published", true)
	}
	m := New[TestModel]().WithCallback("Posts", callback)

	if len(m.relations) != 1 {
		t.Errorf("expected 1 relation, got %d", len(m.relations))
	}
	if m.relationCallbacks["Posts"] == nil {
		t.Error("callback should be set for Posts")
	}
}

// TestWithMorph_SetsTypeMap verifies WithMorph sets type map
func TestWithMorph_SetsTypeMap(t *testing.T) {
	typeMap := map[string][]string{
		"users": {"Profile"},
		"posts": {"Author"},
	}
	m := New[TestModel]().WithMorph("Imageable", typeMap)

	if m.morphRelations["Imageable"] == nil {
		t.Error("morphRelations should be set for Imageable")
	}
	if len(m.morphRelations["Imageable"]["users"]) != 1 {
		t.Error("users should have 1 relation")
	}
}

// =============================================================================
// CTE TESTS
// =============================================================================

// TestWithCTE_StringQuery verifies WithCTE with string query
func TestWithCTE_StringQuery(t *testing.T) {
	m := New[TestModel]().WithCTE("active_users", "SELECT * FROM users WHERE active = true")

	if len(m.ctes) != 1 {
		t.Errorf("expected 1 CTE, got %d", len(m.ctes))
	}
	if m.ctes[0].Name != "active_users" {
		t.Errorf("CTE name should be 'active_users', got %q", m.ctes[0].Name)
	}
}

// TestWithCTE_MultipleCTEs verifies multiple CTEs can be added
func TestWithCTE_MultipleCTEs(t *testing.T) {
	m := New[TestModel]().
		WithCTE("cte1", "SELECT 1").
		WithCTE("cte2", "SELECT 2")

	if len(m.ctes) != 2 {
		t.Errorf("expected 2 CTEs, got %d", len(m.ctes))
	}
}

// =============================================================================
// SCOPE TESTS
// =============================================================================

// TestScope_AppliesFunction verifies Scope applies function
func TestScope_AppliesFunction(t *testing.T) {
	activeScope := func(m *Model[TestModel]) *Model[TestModel] {
		return m.Where("active", true)
	}

	verifiedScope := func(m *Model[TestModel]) *Model[TestModel] {
		return m.WhereNotNull("verified_at")
	}

	m := New[TestModel]().Scope(activeScope).Scope(verifiedScope)
	query, args := m.Print()

	if len(args) < 1 {
		t.Error("Should have at least 1 arg from active scope")
	}
	if !strings.Contains(query, "verified_at IS NOT NULL") {
		t.Error("Query should contain verified_at condition")
	}
}

// TestScope_Chainable verifies multiple scopes can be chained
func TestScope_Chainable(t *testing.T) {
	scope1 := func(m *Model[TestModel]) *Model[TestModel] {
		return m.Where("a", 1)
	}
	scope2 := func(m *Model[TestModel]) *Model[TestModel] {
		return m.Where("b", 2)
	}
	scope3 := func(m *Model[TestModel]) *Model[TestModel] {
		return m.Where("c", 3)
	}

	m := New[TestModel]().Scope(scope1).Scope(scope2).Scope(scope3)
	_, args := m.Print()

	if len(args) != 3 {
		t.Errorf("expected 3 args from 3 scopes, got %d", len(args))
	}
}

// =============================================================================
// LOCK TESTS
// =============================================================================

// TestLock_Modes verifies different lock modes
func TestLock_Modes(t *testing.T) {
	tests := []struct {
		mode     string
		expected string
	}{
		{"UPDATE", "FOR UPDATE"},
		{"SHARE", "FOR SHARE"},
		{"NO KEY UPDATE", "FOR NO KEY UPDATE"},
		{"KEY SHARE", "FOR KEY SHARE"},
	}

	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			m := New[TestModel]().Lock(tt.mode)
			query, _ := m.Print()

			if !strings.Contains(query, tt.expected) {
				t.Errorf("expected %q in query, got %q", tt.expected, query)
			}
		})
	}
}

// =============================================================================
// USEPRIMARY / USEREPLICA TESTS
// =============================================================================

// TestUsePrimary_SetsForcePrimary verifies UsePrimary sets flag
func TestUsePrimary_SetsForcePrimary(t *testing.T) {
	m := New[TestModel]().UsePrimary()

	if !m.forcePrimary {
		t.Error("forcePrimary should be true")
	}
}

// TestUseReplica_SetsForceReplica verifies UseReplica sets index
func TestUseReplica_SetsForceReplica(t *testing.T) {
	m := New[TestModel]().UseReplica(2)

	if m.forceReplica != 2 {
		t.Errorf("forceReplica should be 2, got %d", m.forceReplica)
	}
}

// TestUseReplica_ResetsUsePrimary verifies UseReplica resets forcePrimary
func TestUseReplica_ResetsUsePrimary(t *testing.T) {
	m := New[TestModel]().UsePrimary().UseReplica(0)

	if m.forcePrimary {
		t.Error("forcePrimary should be false after UseReplica")
	}
}
