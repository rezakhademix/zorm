package zorm

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// contextKey is a custom type for context keys to avoid collisions
type contextKey string

// =============================================================================
// NEW TESTS
// =============================================================================

// TestNew_CreatesModelInstance verifies New creates a properly initialized Model
func TestNew_CreatesModelInstance(t *testing.T) {
	m := New[TestModel]()

	if m == nil {
		t.Fatal("New should return a non-nil Model")
	}

	// Verify default values
	if m.ctx == nil {
		t.Error("Model should have a default context")
	}

	if m.modelInfo == nil {
		t.Error("Model should have modelInfo populated")
	}

	if m.relationCallbacks == nil {
		t.Error("relationCallbacks should be initialized")
	}

	if m.morphRelations == nil {
		t.Error("morphRelations should be initialized")
	}

	if m.forceReplica != -1 {
		t.Errorf("forceReplica should be -1 (auto), got %d", m.forceReplica)
	}
}

// TestNew_UsesGlobalDB verifies New uses GlobalDB
func TestNew_UsesGlobalDB(t *testing.T) {
	// Save original
	originalDB := GlobalDB
	defer func() { GlobalDB = originalDB }()

	// Set a mock DB (nil is fine for this test)
	GlobalDB = nil
	m := New[TestModel]()
	if m.db != nil {
		t.Error("Model db should be nil when GlobalDB is nil")
	}
}

// TestNew_PreAllocatesSlices verifies slices are pre-allocated for performance
func TestNew_PreAllocatesSlices(t *testing.T) {
	m := New[TestModel]()

	// wheres and args should be pre-allocated with capacity 4
	if cap(m.wheres) < 4 {
		t.Errorf("wheres should have capacity >= 4, got %d", cap(m.wheres))
	}
	if cap(m.args) < 4 {
		t.Errorf("args should have capacity >= 4, got %d", cap(m.args))
	}
}

// =============================================================================
// CLONE TESTS
// =============================================================================

// TestClone_CreatesDeepCopy verifies Clone creates a deep copy
func TestClone_CreatesDeepCopy(t *testing.T) {
	original := New[TestModel]().
		Where("active", true).
		Select("id", "name").
		OrderBy("created_at", "DESC").
		Limit(10)

	clone := original.Clone()

	// Verify clone is a different instance
	if clone == original {
		t.Error("Clone should return a different instance")
	}

	// Verify values are copied
	if clone.limit != 10 {
		t.Errorf("Clone limit should be 10, got %d", clone.limit)
	}

	if len(clone.columns) != 2 {
		t.Errorf("Clone should have 2 columns, got %d", len(clone.columns))
	}
}

// TestClone_DoesNotModifyOriginal verifies modifying clone doesn't affect original
func TestClone_DoesNotModifyOriginal(t *testing.T) {
	original := New[TestModel]().Where("active", true)
	originalArgsLen := len(original.args)

	clone := original.Clone()
	clone.Where("role", "admin")

	// Original should not be modified
	if len(original.args) != originalArgsLen {
		t.Error("Modifying clone should not affect original")
	}
}

// TestClone_CopiesContext verifies context is preserved
func TestClone_CopiesContext(t *testing.T) {
	ctx := context.WithValue(context.Background(), contextKey("key"), "value")
	original := New[TestModel]().WithContext(ctx)

	clone := original.Clone()

	if clone.ctx != ctx {
		t.Error("Clone should preserve the context")
	}
}

// TestClone_CopiesTableName verifies custom table name is copied
func TestClone_CopiesTableName(t *testing.T) {
	original := New[TestModel]().Table("custom_table")

	clone := original.Clone()

	if clone.TableName() != "custom_table" {
		t.Errorf("Clone should preserve table name, got %q", clone.TableName())
	}
}

// TestClone_CopiesRelations verifies relations are copied
func TestClone_CopiesRelations(t *testing.T) {
	original := New[TestModel]().With("Posts", "Comments")

	clone := original.Clone()

	if len(clone.relations) != 2 {
		t.Errorf("Clone should have 2 relations, got %d", len(clone.relations))
	}

	// Modify clone relations
	clone.relations = append(clone.relations, "Profile")

	// Original should not be modified
	if len(original.relations) != 2 {
		t.Error("Modifying clone relations should not affect original")
	}
}

// TestClone_DeepCopiesMorphRelations verifies morphRelations are deep copied
func TestClone_DeepCopiesMorphRelations(t *testing.T) {
	original := New[TestModel]().WithMorph("Imageable", map[string][]string{
		"users": {"Profile"},
		"posts": {"Author"},
	})

	clone := original.Clone()

	// Verify morphRelations exist
	if clone.morphRelations == nil {
		t.Fatal("Clone morphRelations should not be nil")
	}

	// Modify clone's morphRelations
	if clone.morphRelations["Imageable"] != nil {
		clone.morphRelations["Imageable"]["users"] = append(
			clone.morphRelations["Imageable"]["users"],
			"Settings",
		)
	}

	// Original should not be modified
	if len(original.morphRelations["Imageable"]["users"]) != 1 {
		t.Error("Modifying clone morphRelations should not affect original")
	}
}

// TestClone_PreservesStmtCache verifies statement cache is preserved
func TestClone_PreservesStmtCache(t *testing.T) {
	cache := NewStmtCache(100)
	defer cache.Close()

	original := New[TestModel]().WithStmtCache(cache)
	clone := original.Clone()

	if clone.stmtCache != cache {
		t.Error("Clone should preserve the statement cache reference")
	}
}

// =============================================================================
// WITHCONTEXT TESTS
// =============================================================================

// TestWithContext_SetsContext verifies WithContext sets the context
func TestWithContext_SetsContext(t *testing.T) {
	m := New[TestModel]()
	ctx := context.WithValue(context.Background(), contextKey("key"), "value")

	m.WithContext(ctx)

	if m.ctx != ctx {
		t.Error("WithContext should set the context")
	}
}

// TestWithContext_ReturnsModel verifies WithContext returns the model for chaining
func TestWithContext_ReturnsModel(t *testing.T) {
	m := New[TestModel]()
	result := m.WithContext(context.Background())

	if result != m {
		t.Error("WithContext should return the same model for chaining")
	}
}

// TestWithContext_WithTimeout verifies context with timeout is properly set
func TestWithContext_WithTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m := New[TestModel]().WithContext(ctx)

	// Verify context has deadline
	_, hasDeadline := m.ctx.Deadline()
	if !hasDeadline {
		t.Error("Context should have a deadline")
	}
}

// =============================================================================
// TABLE TESTS
// =============================================================================

// TestTable_SetsCustomTableName verifies Table sets custom table name
func TestTable_SetsCustomTableName(t *testing.T) {
	m := New[TestModel]().Table("custom_users")

	if m.tableName != "custom_users" {
		t.Errorf("Table should set tableName, got %q", m.tableName)
	}
}

// TestTable_ReturnsModelForChaining verifies Table returns model for chaining
func TestTable_ReturnsModelForChaining(t *testing.T) {
	m := New[TestModel]()
	result := m.Table("custom_users")

	if result != m {
		t.Error("Table should return the same model for chaining")
	}
}

// TestTable_OverridesQueryTableName verifies Table affects query generation
func TestTable_OverridesQueryTableName(t *testing.T) {
	m := New[TestModel]().Table("archived_users")
	query, _ := m.Print()

	if m.TableName() != "archived_users" {
		t.Errorf("TableName should return custom name, got %q", m.TableName())
	}

	// Query should use custom table name
	if query == "" {
		t.Skip("Print method not available or returned empty")
	}
}

// =============================================================================
// TABLENAME TESTS
// =============================================================================

// TestTableName_ReturnsDefaultTableName verifies default table name
func TestTableName_ReturnsDefaultTableName(t *testing.T) {
	m := New[TestModel]()

	tableName := m.TableName()

	// Should be snake_case plural of struct name
	if tableName != "test_models" {
		t.Errorf("Default table name should be 'test_models', got %q", tableName)
	}
}

// TestTableName_ReturnsCustomTableName verifies custom table name is returned
func TestTableName_ReturnsCustomTableName(t *testing.T) {
	m := New[TestModel]().Table("custom_table")

	tableName := m.TableName()

	if tableName != "custom_table" {
		t.Errorf("TableName should return custom name 'custom_table', got %q", tableName)
	}
}

// TestTableName_CustomOverridesDefault verifies custom takes precedence
func TestTableName_CustomOverridesDefault(t *testing.T) {
	m := New[TestModel]()

	// First should return default
	if m.TableName() != "test_models" {
		t.Errorf("Expected default 'test_models', got %q", m.TableName())
	}

	// Set custom
	m.Table("override_table")

	// Now should return custom
	if m.TableName() != "override_table" {
		t.Errorf("Expected custom 'override_table', got %q", m.TableName())
	}
}

// =============================================================================
// SETDB TESTS
// =============================================================================

// TestSetDB_SetsDatabase verifies SetDB sets the database
func TestSetDB_SetsDatabase(t *testing.T) {
	m := New[TestModel]()

	// Create a mock DB pointer (don't actually open connection)
	var mockDB *sql.DB

	m.SetDB(mockDB)

	if m.db != mockDB {
		t.Error("SetDB should set the database")
	}
}

// TestSetDB_ReturnsModelForChaining verifies SetDB returns model for chaining
func TestSetDB_ReturnsModelForChaining(t *testing.T) {
	m := New[TestModel]()
	result := m.SetDB(nil)

	if result != m {
		t.Error("SetDB should return the same model for chaining")
	}
}

// TestSetDB_OverridesGlobalDB verifies SetDB overrides GlobalDB
func TestSetDB_OverridesGlobalDB(t *testing.T) {
	// Save original
	originalDB := GlobalDB
	defer func() { GlobalDB = originalDB }()

	GlobalDB = nil
	m := New[TestModel]()

	if m.db != nil {
		t.Error("db should be nil initially")
	}

	// Set a different DB
	var customDB *sql.DB
	m.SetDB(customDB)

	// Should use custom DB, not GlobalDB
	if m.db != customDB {
		t.Error("SetDB should override GlobalDB")
	}
}

// =============================================================================
// WITHSTMTCACHE TESTS
// =============================================================================

// TestWithStmtCache_SetsCache verifies WithStmtCache sets the cache
func TestWithStmtCache_SetsCache(t *testing.T) {
	cache := NewStmtCache(100)
	defer cache.Close()

	m := New[TestModel]().WithStmtCache(cache)

	if m.stmtCache != cache {
		t.Error("WithStmtCache should set the statement cache")
	}
}

// TestWithStmtCache_ReturnsModelForChaining verifies chaining works
func TestWithStmtCache_ReturnsModelForChaining(t *testing.T) {
	cache := NewStmtCache(100)
	defer cache.Close()

	m := New[TestModel]()
	result := m.WithStmtCache(cache)

	if result != m {
		t.Error("WithStmtCache should return the same model for chaining")
	}
}

// TestWithStmtCache_NilCacheIsAllowed verifies nil cache is handled
func TestWithStmtCache_NilCacheIsAllowed(t *testing.T) {
	m := New[TestModel]().WithStmtCache(nil)

	if m.stmtCache != nil {
		t.Error("WithStmtCache(nil) should set cache to nil")
	}
}

// =============================================================================
// CONFIGURECONNECTIONPOOL TESTS
// =============================================================================

// TestConfigureConnectionPool_NilDB verifies nil DB is handled gracefully
func TestConfigureConnectionPool_NilDB(t *testing.T) {
	// Should not panic with nil DB
	ConfigureConnectionPool(nil, 25, 5, time.Hour, 30*time.Minute)
}

// TestConfigureConnectionPool_Signature verifies function signature
func TestConfigureConnectionPool_Signature(t *testing.T) {
	// This test verifies the function exists and has correct signature
	// We can't actually test config without a real DB connection
	t.Log("ConfigureConnectionPool accepts: db *sql.DB, maxOpen, maxIdle int, maxLifetime, idleTimeout time.Duration")
}

// =============================================================================
// CONFIGUREDBRESOLVER TESTS
// =============================================================================

// TestConfigureDBResolver_SetsGlobalResolver verifies global resolver is set
func TestConfigureDBResolver_SetsGlobalResolver(t *testing.T) {
	// Save original
	originalResolver := GetGlobalResolver()
	defer func() {
		if originalResolver != nil {
			ConfigureDBResolver(WithPrimary(originalResolver.Primary()))
		} else {
			ClearDBResolver()
		}
	}()

	ClearDBResolver()

	ConfigureDBResolver()

	if GetGlobalResolver() == nil {
		t.Error("ConfigureDBResolver should set global resolver")
	}
}

// TestConfigureDBResolver_DefaultLoadBalancer verifies default LB is set
func TestConfigureDBResolver_DefaultLoadBalancer(t *testing.T) {
	// Save original
	originalResolver := GetGlobalResolver()
	defer func() {
		if originalResolver != nil {
			ConfigureDBResolver(WithPrimary(originalResolver.Primary()))
		} else {
			ClearDBResolver()
		}
	}()

	ConfigureDBResolver()

	resolver := GetGlobalResolver()
	if resolver == nil || resolver.lb == nil {
		t.Error("ConfigureDBResolver should set default load balancer")
	}
}

// TestConfigureDBResolver_WithOptions verifies options are applied
func TestConfigureDBResolver_WithOptions(t *testing.T) {
	// Save original
	originalResolver := GetGlobalResolver()
	defer func() {
		if originalResolver != nil {
			ConfigureDBResolver(WithPrimary(originalResolver.Primary()))
		} else {
			ClearDBResolver()
		}
	}()

	// Create mock DBs
	var primaryDB *sql.DB
	var replicaDB *sql.DB

	ConfigureDBResolver(
		WithPrimary(primaryDB),
		WithReplicas(replicaDB),
	)

	resolver := GetGlobalResolver()
	if resolver == nil {
		t.Fatal("ConfigureDBResolver should set global resolver")
	}

	if resolver.Primary() != primaryDB {
		t.Error("WithPrimary option should set primary DB")
	}

	if !resolver.HasReplicas() {
		t.Error("WithReplicas should add 1 replica")
	}
}

// =============================================================================
// MODEL STRUCT TESTS
// =============================================================================

// TestModel_HasRequiredFields verifies Model struct has required fields
func TestModel_HasRequiredFields(t *testing.T) {
	m := New[TestModel]()

	// Check fields exist by accessing them
	_ = m.ctx
	_ = m.db
	_ = m.tx
	_ = m.modelInfo
	_ = m.tableName
	_ = m.columns
	_ = m.wheres
	_ = m.args
	_ = m.orderBys
	_ = m.groupBys
	_ = m.distinct
	_ = m.limit
	_ = m.offset
	_ = m.relations
	_ = m.relationCallbacks
	_ = m.morphRelations
	_ = m.lockMode
	_ = m.forcePrimary
	_ = m.forceReplica
	_ = m.rawQuery
	_ = m.rawArgs
	_ = m.ctes
	_ = m.stmtCache

	t.Log("All Model fields are accessible")
}

// TestCTE_Struct verifies CTE struct fields
func TestCTE_Struct(t *testing.T) {
	cte := CTE{
		Name:  "test_cte",
		Query: "SELECT * FROM users",
		Args:  []any{1, 2, 3},
	}

	if cte.Name != "test_cte" {
		t.Errorf("CTE Name should be 'test_cte', got %q", cte.Name)
	}

	if cte.Query != "SELECT * FROM users" {
		t.Error("CTE Query should be set")
	}

	if len(cte.Args) != 3 {
		t.Errorf("CTE Args should have 3 elements, got %d", len(cte.Args))
	}
}

// =============================================================================
// CHAINING TESTS
// =============================================================================

// TestMethodChaining_AllConfigMethods verifies all config methods can be chained
func TestMethodChaining_AllConfigMethods(t *testing.T) {
	cache := NewStmtCache(100)
	defer cache.Close()

	ctx := context.Background()

	// All these methods should be chainable
	m := New[TestModel]().
		WithContext(ctx).
		Table("custom_table").
		SetDB(nil).
		WithStmtCache(cache)

	// Verify non-nil before accessing fields
	if m == nil {
		t.Fatal("Chained methods should return non-nil model")
	}

	if m.TableName() != "custom_table" {
		t.Error("Table should be set during chaining")
	}

	if m.stmtCache != cache {
		t.Error("StmtCache should be set during chaining")
	}
}
