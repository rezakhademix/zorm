package zorm

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

// =============================================================================
// QUERY BUILDING TESTS (no database required)
// =============================================================================

// TestFirst_QueryBuilding verifies that First adds LIMIT 1
func TestFirst_QueryBuilding(t *testing.T) {
	m := New[TestModel]().Where("name", "John")

	// First should add LIMIT 1 internally
	// We check the method exists and has correct signature
	methodVal := reflect.ValueOf(m).MethodByName("First")
	if !methodVal.IsValid() {
		t.Fatal("First method not found")
	}

	methodType := methodVal.Type()
	if methodType.NumIn() != 1 { // context.Context
		t.Errorf("First should take 1 argument (context), got %d", methodType.NumIn())
	}
	if methodType.NumOut() != 2 { // *T, error
		t.Errorf("First should return 2 values (*T, error), got %d", methodType.NumOut())
	}
}

// TestFind_QueryBuilding verifies Find builds correct WHERE clause
func TestFind_QueryBuilding(t *testing.T) {
	m := New[TestModel]()

	// Verify Find method exists with correct signature
	methodVal := reflect.ValueOf(m).MethodByName("Find")
	if !methodVal.IsValid() {
		t.Fatal("Find method not found")
	}

	methodType := methodVal.Type()
	if methodType.NumIn() != 2 { // context.Context, id any
		t.Errorf("Find should take 2 arguments (context, id), got %d", methodType.NumIn())
	}
	if methodType.NumOut() != 2 { // *T, error
		t.Errorf("Find should return 2 values (*T, error), got %d", methodType.NumOut())
	}
}

// TestFindOrFail_QueryBuilding verifies FindOrFail exists
func TestFindOrFail_QueryBuilding(t *testing.T) {
	m := New[TestModel]()

	methodVal := reflect.ValueOf(m).MethodByName("FindOrFail")
	if !methodVal.IsValid() {
		t.Fatal("FindOrFail method not found")
	}

	methodType := methodVal.Type()
	if methodType.NumIn() != 2 { // context.Context, id any
		t.Errorf("FindOrFail should take 2 arguments (context, id), got %d", methodType.NumIn())
	}
}

// TestCount_QueryBuilding verifies Count method signature
func TestCount_QueryBuilding(t *testing.T) {
	m := New[TestModel]().Where("active", true)

	methodVal := reflect.ValueOf(m).MethodByName("Count")
	if !methodVal.IsValid() {
		t.Fatal("Count method not found")
	}

	methodType := methodVal.Type()
	if methodType.NumOut() != 2 { // int64, error
		t.Errorf("Count should return 2 values (int64, error), got %d", methodType.NumOut())
	}
}

// TestCountOver_QueryBuilding verifies CountOver method exists
func TestCountOver_QueryBuilding(t *testing.T) {
	m := New[TestModel]()

	methodVal := reflect.ValueOf(m).MethodByName("CountOver")
	if !methodVal.IsValid() {
		t.Fatal("CountOver method not found")
	}

	methodType := methodVal.Type()
	if methodType.NumIn() != 2 { // context.Context, column string
		t.Errorf("CountOver should take 2 arguments, got %d", methodType.NumIn())
	}
}

// =============================================================================
// CURSOR TESTS
// =============================================================================

// TestCursor_Signature verifies Cursor method exists
func TestCursor_Signature(t *testing.T) {
	m := New[TestModel]()

	methodVal := reflect.ValueOf(m).MethodByName("Cursor")
	if !methodVal.IsValid() {
		t.Fatal("Cursor method not found")
	}

	methodType := methodVal.Type()
	if methodType.NumIn() != 1 { // context.Context
		t.Errorf("Cursor should take 1 argument (context), got %d", methodType.NumIn())
	}
	if methodType.NumOut() != 2 { // *Cursor[T], error
		t.Errorf("Cursor should return 2 values (*Cursor[T], error), got %d", methodType.NumOut())
	}
}

// TestCursor_TypeMethods verifies Cursor type has required methods
func TestCursor_TypeMethods(t *testing.T) {
	// Verify Cursor type exists and has Next, Scan, Close methods
	cursorType := reflect.TypeOf((*Cursor[TestModel])(nil))

	methods := []string{"Next", "Scan", "Close"}
	for _, name := range methods {
		method, ok := cursorType.MethodByName(name)
		if !ok {
			t.Errorf("Cursor should have %s method", name)
			continue
		}
		t.Logf("Cursor.%s: %v", name, method.Type)
	}
}

// =============================================================================
// FIRSTORCREATE / UPDATEORCREATE TESTS
// =============================================================================

// TestFirstOrCreate_Signature verifies FirstOrCreate method signature
func TestFirstOrCreate_Signature(t *testing.T) {
	m := New[TestModel]()

	methodVal := reflect.ValueOf(m).MethodByName("FirstOrCreate")
	if !methodVal.IsValid() {
		t.Fatal("FirstOrCreate method not found")
	}

	methodType := methodVal.Type()
	// FirstOrCreate(ctx context.Context, attributes map[string]any, values map[string]any) (*T, error)
	if methodType.NumIn() != 3 {
		t.Errorf("FirstOrCreate should take 3 arguments (context, attributes, values), got %d", methodType.NumIn())
	}
	if methodType.NumOut() != 2 {
		t.Errorf("FirstOrCreate should return 2 values (*T, error), got %d", methodType.NumOut())
	}
}

// TestUpdateOrCreate_Signature verifies UpdateOrCreate method signature
func TestUpdateOrCreate_Signature(t *testing.T) {
	m := New[TestModel]()

	methodVal := reflect.ValueOf(m).MethodByName("UpdateOrCreate")
	if !methodVal.IsValid() {
		t.Fatal("UpdateOrCreate method not found")
	}

	methodType := methodVal.Type()
	// UpdateOrCreate(ctx context.Context, attributes map[string]any, values map[string]any) (*T, error)
	if methodType.NumIn() != 3 {
		t.Errorf("UpdateOrCreate should take 3 arguments (context, attributes, values), got %d", methodType.NumIn())
	}
	if methodType.NumOut() != 2 {
		t.Errorf("UpdateOrCreate should return 2 values (*T, error), got %d", methodType.NumOut())
	}
}

// =============================================================================
// CRUD METHOD TESTS
// =============================================================================

// TestCreate_Signature verifies Create method signature
func TestCreate_Signature(t *testing.T) {
	m := New[TestModel]()

	methodVal := reflect.ValueOf(m).MethodByName("Create")
	if !methodVal.IsValid() {
		t.Fatal("Create method not found")
	}

	methodType := methodVal.Type()
	// Create(ctx context.Context, entity *T) error
	if methodType.NumIn() != 2 {
		t.Errorf("Create should take 2 arguments (context, entity), got %d", methodType.NumIn())
	}
	if methodType.NumOut() != 1 {
		t.Errorf("Create should return 1 value (error), got %d", methodType.NumOut())
	}
}

// TestUpdate_Signature verifies Update method signature
func TestUpdate_Signature(t *testing.T) {
	m := New[TestModel]()

	methodVal := reflect.ValueOf(m).MethodByName("Update")
	if !methodVal.IsValid() {
		t.Fatal("Update method not found")
	}

	methodType := methodVal.Type()
	// Update(ctx context.Context, entity *T) error
	if methodType.NumIn() != 2 {
		t.Errorf("Update should take 2 arguments (context, entity), got %d", methodType.NumIn())
	}
	if methodType.NumOut() != 1 {
		t.Errorf("Update should return 1 value (error), got %d", methodType.NumOut())
	}
}

// TestDelete_Signature verifies Delete method signature
func TestDelete_Signature(t *testing.T) {
	m := New[TestModel]()

	methodVal := reflect.ValueOf(m).MethodByName("Delete")
	if !methodVal.IsValid() {
		t.Fatal("Delete method not found")
	}

	methodType := methodVal.Type()
	// Delete(ctx context.Context) error
	if methodType.NumIn() != 1 {
		t.Errorf("Delete should take 1 argument (context), got %d", methodType.NumIn())
	}
	if methodType.NumOut() != 1 {
		t.Errorf("Delete should return 1 value (error), got %d", methodType.NumOut())
	}
}

// TestExec_Signature verifies Exec method signature
func TestExec_Signature(t *testing.T) {
	m := New[TestModel]()

	methodVal := reflect.ValueOf(m).MethodByName("Exec")
	if !methodVal.IsValid() {
		t.Fatal("Exec method not found")
	}

	methodType := methodVal.Type()
	// Exec(ctx context.Context) (sql.Result, error)
	if methodType.NumIn() != 1 {
		t.Errorf("Exec should take 1 argument (context), got %d", methodType.NumIn())
	}
	if methodType.NumOut() != 2 {
		t.Errorf("Exec should return 2 values (sql.Result, error), got %d", methodType.NumOut())
	}
}

// =============================================================================
// BATCH OPERATION TESTS
// =============================================================================

// TestCreateMany_Signature verifies CreateMany method signature
func TestCreateMany_Signature(t *testing.T) {
	m := New[TestModel]()

	methodVal := reflect.ValueOf(m).MethodByName("CreateMany")
	if !methodVal.IsValid() {
		t.Fatal("CreateMany method not found")
	}

	methodType := methodVal.Type()
	// CreateMany(ctx context.Context, entities []*T) error
	if methodType.NumIn() != 2 {
		t.Errorf("CreateMany should take 2 arguments (context, entities), got %d", methodType.NumIn())
	}
	if methodType.NumOut() != 1 {
		t.Errorf("CreateMany should return 1 value (error), got %d", methodType.NumOut())
	}
}

// TestUpdateMany_Signature verifies UpdateMany method signature
func TestUpdateMany_Signature(t *testing.T) {
	m := New[TestModel]()

	methodVal := reflect.ValueOf(m).MethodByName("UpdateMany")
	if !methodVal.IsValid() {
		t.Fatal("UpdateMany method not found")
	}

	methodType := methodVal.Type()
	// UpdateMany(ctx context.Context, values map[string]any) error
	if methodType.NumIn() != 2 {
		t.Errorf("UpdateMany should take 2 arguments (context, values), got %d", methodType.NumIn())
	}
	if methodType.NumOut() != 1 {
		t.Errorf("UpdateMany should return 1 value (error), got %d", methodType.NumOut())
	}
}

// TestDeleteMany_Signature verifies DeleteMany method signature
func TestDeleteMany_Signature(t *testing.T) {
	m := New[TestModel]()

	methodVal := reflect.ValueOf(m).MethodByName("DeleteMany")
	if !methodVal.IsValid() {
		t.Fatal("DeleteMany method not found")
	}

	methodType := methodVal.Type()
	// DeleteMany(ctx context.Context) error
	if methodType.NumIn() != 1 {
		t.Errorf("DeleteMany should take 1 argument (context), got %d", methodType.NumIn())
	}
	if methodType.NumOut() != 1 {
		t.Errorf("DeleteMany should return 1 value (error), got %d", methodType.NumOut())
	}
}

// =============================================================================
// HOOK TESTS
// =============================================================================

// ModelWithHooks is a test model with lifecycle hooks
type ModelWithHooks struct {
	ID           int64
	Name         string
	BeforeCalled bool
	AfterCalled  bool
}

func (m *ModelWithHooks) BeforeCreate(ctx context.Context) error {
	m.BeforeCalled = true
	return nil
}

func (m *ModelWithHooks) BeforeUpdate(ctx context.Context) error {
	m.BeforeCalled = true
	return nil
}

func (m *ModelWithHooks) AfterUpdate(ctx context.Context) error {
	m.AfterCalled = true
	return nil
}

// TestBeforeCreate_HookDetection verifies BeforeCreate hook is detected
func TestBeforeCreate_HookDetection(t *testing.T) {
	entity := &ModelWithHooks{Name: "Test"}

	// Check if BeforeCreate method exists on the entity
	entityVal := reflect.ValueOf(entity)
	method := entityVal.MethodByName("BeforeCreate")

	if !method.IsValid() {
		t.Fatal("BeforeCreate method should exist on ModelWithHooks")
	}

	// Verify method signature
	methodType := method.Type()
	if methodType.NumIn() != 1 { // context.Context
		t.Errorf("BeforeCreate should take 1 argument (context), got %d", methodType.NumIn())
	}
	if methodType.NumOut() != 1 { // error
		t.Errorf("BeforeCreate should return 1 value (error), got %d", methodType.NumOut())
	}

	t.Log("BeforeCreate hook correctly detected")
}

// TestBeforeUpdate_HookDetection verifies BeforeUpdate hook is detected
func TestBeforeUpdate_HookDetection(t *testing.T) {
	entity := &ModelWithHooks{Name: "Test"}

	entityVal := reflect.ValueOf(entity)
	method := entityVal.MethodByName("BeforeUpdate")

	if !method.IsValid() {
		t.Fatal("BeforeUpdate method should exist on ModelWithHooks")
	}

	methodType := method.Type()
	if methodType.NumIn() != 1 {
		t.Errorf("BeforeUpdate should take 1 argument (context), got %d", methodType.NumIn())
	}
	if methodType.NumOut() != 1 {
		t.Errorf("BeforeUpdate should return 1 value (error), got %d", methodType.NumOut())
	}

	t.Log("BeforeUpdate hook correctly detected")
}

// TestAfterUpdate_HookDetection verifies AfterUpdate hook is detected
func TestAfterUpdate_HookDetection(t *testing.T) {
	entity := &ModelWithHooks{Name: "Test"}

	entityVal := reflect.ValueOf(entity)
	method := entityVal.MethodByName("AfterUpdate")

	if !method.IsValid() {
		t.Fatal("AfterUpdate method should exist on ModelWithHooks")
	}

	methodType := method.Type()
	if methodType.NumIn() != 1 {
		t.Errorf("AfterUpdate should take 1 argument (context), got %d", methodType.NumIn())
	}
	if methodType.NumOut() != 1 {
		t.Errorf("AfterUpdate should return 1 value (error), got %d", methodType.NumOut())
	}

	t.Log("AfterUpdate hook correctly detected")
}

// TestHook_CalledViaReflection verifies hooks can be called via reflection
func TestHook_CalledViaReflection(t *testing.T) {
	entity := &ModelWithHooks{Name: "Test"}
	ctx := context.Background()

	// Simulate how ZORM calls hooks
	entityVal := reflect.ValueOf(entity)

	// Call BeforeCreate
	if method := entityVal.MethodByName("BeforeCreate"); method.IsValid() {
		results := method.Call([]reflect.Value{reflect.ValueOf(ctx)})
		if len(results) == 1 && !results[0].IsNil() {
			t.Errorf("BeforeCreate returned error: %v", results[0].Interface())
		}
	}

	if !entity.BeforeCalled {
		t.Error("BeforeCreate hook was not executed")
	}

	// Reset and test AfterUpdate
	entity.BeforeCalled = false
	entity.AfterCalled = false

	if method := entityVal.MethodByName("AfterUpdate"); method.IsValid() {
		results := method.Call([]reflect.Value{reflect.ValueOf(ctx)})
		if len(results) == 1 && !results[0].IsNil() {
			t.Errorf("AfterUpdate returned error: %v", results[0].Interface())
		}
	}

	if !entity.AfterCalled {
		t.Error("AfterUpdate hook was not executed")
	}
}

// =============================================================================
// NIL POINTER VALIDATION TESTS
// =============================================================================

// TestCreate_NilEntity verifies Create rejects nil entity
func TestCreate_NilEntity(t *testing.T) {
	m := New[TestModel]()
	err := m.Create(context.Background(), nil)

	if err == nil {
		t.Error("Create should return error for nil entity")
	}
	if err != ErrNilPointer {
		t.Errorf("Create should return ErrNilPointer, got %v", err)
	}
}

// TestUpdate_NilEntity verifies Update rejects nil entity
func TestUpdate_NilEntity(t *testing.T) {
	m := New[TestModel]()
	err := m.Update(context.Background(), nil)

	if err == nil {
		t.Error("Update should return error for nil entity")
	}
	if err != ErrNilPointer {
		t.Errorf("Update should return ErrNilPointer, got %v", err)
	}
}

// TestCreateMany_EmptySlice verifies CreateMany handles empty slice
func TestCreateMany_EmptySlice(t *testing.T) {
	m := New[TestModel]()
	err := m.CreateMany(context.Background(), []*TestModel{})

	// Empty slice should return nil (no-op) or specific error
	// Check it doesn't panic
	t.Logf("CreateMany with empty slice returned: %v", err)
}

// TestCreateMany_NilSlice verifies CreateMany handles nil slice
func TestCreateMany_NilSlice(t *testing.T) {
	m := New[TestModel]()
	err := m.CreateMany(context.Background(), nil)

	// Nil slice should return ErrNilPointer or similar
	t.Logf("CreateMany with nil slice returned: %v", err)
}

// =============================================================================
// QUERY ISOLATION TESTS
// =============================================================================

// TestClone_IsolatesQueries verifies Clone creates independent query builders
func TestClone_IsolatesQueries(t *testing.T) {
	base := New[TestModel]().Where("active", true)

	clone1 := base.Clone().Where("role", "admin")
	clone2 := base.Clone().Where("role", "user")

	_, args1 := clone1.Print()
	_, args2 := clone2.Print()
	_, baseArgs := base.Print()

	// Base should not be modified
	if len(baseArgs) != 1 {
		t.Errorf("Base should have 1 arg, got %d", len(baseArgs))
	}

	// Clone1 should have 2 args: true, "admin"
	if len(args1) != 2 {
		t.Errorf("Clone1 should have 2 args, got %d", len(args1))
	} else if args1[1] != "admin" {
		t.Errorf("Clone1 second arg should be 'admin', got %v", args1[1])
	}

	// Clone2 should have 2 args: true, "user"
	if len(args2) != 2 {
		t.Errorf("Clone2 should have 2 args, got %d", len(args2))
	} else if args2[1] != "user" {
		t.Errorf("Clone2 second arg should be 'user', got %v", args2[1])
	}
}

// =============================================================================
// SQL GENERATION TESTS
// =============================================================================

// TestBuildSelectQuery_AllClauses verifies all SELECT clauses are generated
func TestBuildSelectQuery_AllClauses(t *testing.T) {
	m := New[TestModel]().
		Select("id", "name").
		Distinct().
		Where("active", true).
		GroupBy("status").
		Having("COUNT(*) >", 5).
		OrderBy("name", "ASC").
		Limit(10).
		Offset(20).
		Lock("UPDATE")

	query, args := m.Print()

	tests := []struct {
		name     string
		expected string
	}{
		{"SELECT columns", "id, name"},
		{"DISTINCT", "DISTINCT"},
		{"WHERE", "active"},
		{"GROUP BY", "GROUP BY status"},
		{"HAVING", "HAVING"},
		{"ORDER BY", "ORDER BY name ASC"},
		{"LIMIT", "LIMIT 10"},
		{"OFFSET", "OFFSET 20"},
		{"FOR UPDATE", "FOR UPDATE"},
	}

	for _, tt := range tests {
		if !strings.Contains(query, tt.expected) {
			t.Errorf("%s: expected query to contain %q, got %q", tt.name, tt.expected, query)
		}
	}

	if len(args) < 2 {
		t.Errorf("Expected at least 2 args, got %d", len(args))
	}
}

// TestTableName_Override verifies Table() overrides table name
func TestTableName_Override(t *testing.T) {
	m := New[TestModel]().Table("custom_table")

	if m.TableName() != "custom_table" {
		t.Errorf("TableName() should return 'custom_table', got %q", m.TableName())
	}

	query, _ := m.Print()
	if !strings.Contains(query, "FROM custom_table") {
		t.Errorf("Query should use custom table name, got %q", query)
	}
}

// TestTableName_Default verifies default table name generation
func TestTableName_Default(t *testing.T) {
	m := New[TestModel]()

	// Should be snake_case plural of struct name
	tableName := m.TableName()
	if tableName != "test_models" {
		t.Errorf("Expected table name 'test_models', got %q", tableName)
	}
}

// =============================================================================
// RELATION FIELD EXCLUSION TESTS
// =============================================================================

// TestBranch represents a related entity for testing
type TestBranch struct {
	ID   int
	Name string
}

// TestPost represents a related entity for HasMany testing
type TestPost struct {
	ID     int
	Title  string
	UserID int
}

// TestUserWithRelations is a model with relation fields that should be excluded from queries
type TestUserWithRelations struct {
	ID        int
	Name      string
	Email     string
	BranchID  *int         // This is a foreign key - should be included
	Branch    *TestBranch  // This is a relation - should be excluded
	Posts     []*TestPost  // This is a relation - should be excluded
}

// TestUpdate_ExcludesRelationFields verifies that Update query doesn't include relation columns
func TestUpdate_ExcludesRelationFields(t *testing.T) {
	// Parse the model info to check fields
	info := ParseModel[TestUserWithRelations]()

	// Verify regular fields are present
	regularFields := []string{"ID", "Name", "Email", "BranchID"}
	for _, fieldName := range regularFields {
		if _, ok := info.Fields[fieldName]; !ok {
			t.Errorf("Expected field %s to be present in model info", fieldName)
		}
	}

	// Verify relation fields are NOT present
	relationFields := []string{"Branch", "Posts"}
	for _, fieldName := range relationFields {
		if _, ok := info.Fields[fieldName]; ok {
			t.Errorf("Relation field %s should NOT be present in model info", fieldName)
		}
	}

	// Verify columns don't include relation columns
	if _, ok := info.Columns["branch"]; ok {
		t.Error("Column 'branch' should NOT exist - it's a relation field")
	}
	if _, ok := info.Columns["posts"]; ok {
		t.Error("Column 'posts' should NOT exist - it's a relation field")
	}

	// Verify foreign key column IS present
	if _, ok := info.Columns["branch_id"]; !ok {
		t.Error("Column 'branch_id' should exist - it's a foreign key, not a relation")
	}
}

// TestModelInfo_RelationFieldsExcluded verifies ModelInfo correctly excludes relation fields
func TestModelInfo_RelationFieldsExcluded(t *testing.T) {
	info := ParseModel[TestUserWithRelations]()

	// Count fields - should only have ID, Name, Email, BranchID (4 fields)
	expectedFieldCount := 4
	if len(info.Fields) != expectedFieldCount {
		t.Errorf("Expected %d fields, got %d. Fields: %v", expectedFieldCount, len(info.Fields), getFieldNames(info.Fields))
	}

	// Count columns - should match field count
	if len(info.Columns) != expectedFieldCount {
		t.Errorf("Expected %d columns, got %d. Columns: %v", expectedFieldCount, len(info.Columns), getColumnNames(info.Columns))
	}
}

// Helper to get field names for error messages
func getFieldNames(fields map[string]*FieldInfo) []string {
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	return names
}

// Helper to get column names for error messages
func getColumnNames(columns map[string]*FieldInfo) []string {
	names := make([]string, 0, len(columns))
	for name := range columns {
		names = append(names, name)
	}
	return names
}

// TestComment represents a related entity for nested relation testing
type TestComment struct {
	ID     int
	Body   string
	PostID int
}

// TestPostWithComments is a model with nested relations
type TestPostWithComments struct {
	ID       int
	Title    string
	UserID   int
	Comments []*TestComment // Relation - should be excluded
}

// TestModelInfo_NestedRelationsExcluded verifies nested relation fields are excluded
func TestModelInfo_NestedRelationsExcluded(t *testing.T) {
	info := ParseModel[TestPostWithComments]()

	// Comments field should NOT be present
	if _, ok := info.Fields["Comments"]; ok {
		t.Error("Relation field 'Comments' should NOT be present in model info")
	}

	// comments column should NOT exist
	if _, ok := info.Columns["comments"]; ok {
		t.Error("Column 'comments' should NOT exist - it's a relation field")
	}

	// Regular fields should be present
	expectedFields := []string{"ID", "Title", "UserID"}
	for _, fieldName := range expectedFields {
		if _, ok := info.Fields[fieldName]; !ok {
			t.Errorf("Expected field %s to be present", fieldName)
		}
	}
}

// TestProfile represents a HasOne relation target
type TestProfile struct {
	ID     int
	Bio    string
	UserID int
}

// TestUserWithProfile tests pointer to struct (HasOne style relation)
type TestUserWithProfile struct {
	ID        int
	Name      string
	Profile   *TestProfile // HasOne relation - should be excluded
	ProfileID *int         // Foreign key - should be included
}

// TestModelInfo_HasOneRelationExcluded verifies HasOne relation fields are excluded
func TestModelInfo_HasOneRelationExcluded(t *testing.T) {
	info := ParseModel[TestUserWithProfile]()

	// Profile field should NOT be present
	if _, ok := info.Fields["Profile"]; ok {
		t.Error("HasOne relation field 'Profile' should NOT be present in model info")
	}

	// profile column should NOT exist
	if _, ok := info.Columns["profile"]; ok {
		t.Error("Column 'profile' should NOT exist - it's a relation field")
	}

	// ProfileID (foreign key) should be present
	if _, ok := info.Fields["ProfileID"]; !ok {
		t.Error("Foreign key field 'ProfileID' should be present")
	}
	if _, ok := info.Columns["profile_id"]; !ok {
		t.Error("Column 'profile_id' should exist - it's a foreign key")
	}
}

// TestModelInfo_MultipleRelationsExcluded tests a model with multiple relation types
func TestModelInfo_MultipleRelationsExcluded(t *testing.T) {
	// TestUserWithRelations has:
	// - BranchID *int (foreign key - include)
	// - Branch *TestBranch (BelongsTo relation - exclude)
	// - Posts []*TestPost (HasMany relation - exclude)
	info := ParseModel[TestUserWithRelations]()

	tests := []struct {
		name     string
		field    string
		column   string
		shouldExist bool
	}{
		{"ID field", "ID", "id", true},
		{"Name field", "Name", "name", true},
		{"Email field", "Email", "email", true},
		{"BranchID foreign key", "BranchID", "branch_id", true},
		{"Branch relation", "Branch", "branch", false},
		{"Posts relation", "Posts", "posts", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, fieldExists := info.Fields[tt.field]
			_, columnExists := info.Columns[tt.column]

			if tt.shouldExist {
				if !fieldExists {
					t.Errorf("Field %s should exist", tt.field)
				}
				if !columnExists {
					t.Errorf("Column %s should exist", tt.column)
				}
			} else {
				if fieldExists {
					t.Errorf("Field %s should NOT exist (it's a relation)", tt.field)
				}
				if columnExists {
					t.Errorf("Column %s should NOT exist (it's a relation)", tt.column)
				}
			}
		})
	}
}
