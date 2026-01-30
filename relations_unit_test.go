package zorm

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/google/uuid"
)

// ==================== anyToKeyString Tests ====================

func TestAnyToKeyString_Int64(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{-1, "-1"},
		{9223372036854775807, "9223372036854775807"},
		{-9223372036854775808, "-9223372036854775808"},
	}

	for _, tc := range tests {
		result := anyToKeyString(tc.input)
		if result != tc.expected {
			t.Errorf("anyToKeyString(%d) = %q, want %q", tc.input, result, tc.expected)
		}
	}
}

func TestAnyToKeyString_Int(t *testing.T) {
	tests := []struct {
		input    int
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{-1, "-1"},
		{42, "42"},
		{-999, "-999"},
	}

	for _, tc := range tests {
		result := anyToKeyString(tc.input)
		if result != tc.expected {
			t.Errorf("anyToKeyString(%d) = %q, want %q", tc.input, result, tc.expected)
		}
	}
}

func TestAnyToKeyString_Int32(t *testing.T) {
	tests := []struct {
		input    int32
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{-1, "-1"},
		{2147483647, "2147483647"},
		{-2147483648, "-2147483648"},
	}

	for _, tc := range tests {
		result := anyToKeyString(tc.input)
		if result != tc.expected {
			t.Errorf("anyToKeyString(%d) = %q, want %q", tc.input, result, tc.expected)
		}
	}
}

func TestAnyToKeyString_Int16(t *testing.T) {
	tests := []struct {
		input    int16
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{-1, "-1"},
		{32767, "32767"},
		{-32768, "-32768"},
	}

	for _, tc := range tests {
		result := anyToKeyString(tc.input)
		if result != tc.expected {
			t.Errorf("anyToKeyString(%d) = %q, want %q", tc.input, result, tc.expected)
		}
	}
}

func TestAnyToKeyString_Int8(t *testing.T) {
	tests := []struct {
		input    int8
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{-1, "-1"},
		{127, "127"},
		{-128, "-128"},
	}

	for _, tc := range tests {
		result := anyToKeyString(tc.input)
		if result != tc.expected {
			t.Errorf("anyToKeyString(%d) = %q, want %q", tc.input, result, tc.expected)
		}
	}
}

func TestAnyToKeyString_Uint64(t *testing.T) {
	tests := []struct {
		input    uint64
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{18446744073709551615, "18446744073709551615"},
	}

	for _, tc := range tests {
		result := anyToKeyString(tc.input)
		if result != tc.expected {
			t.Errorf("anyToKeyString(%d) = %q, want %q", tc.input, result, tc.expected)
		}
	}
}

func TestAnyToKeyString_Uint(t *testing.T) {
	tests := []struct {
		input    uint
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{42, "42"},
		{999, "999"},
	}

	for _, tc := range tests {
		result := anyToKeyString(tc.input)
		if result != tc.expected {
			t.Errorf("anyToKeyString(%d) = %q, want %q", tc.input, result, tc.expected)
		}
	}
}

func TestAnyToKeyString_Uint32(t *testing.T) {
	tests := []struct {
		input    uint32
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{4294967295, "4294967295"},
	}

	for _, tc := range tests {
		result := anyToKeyString(tc.input)
		if result != tc.expected {
			t.Errorf("anyToKeyString(%d) = %q, want %q", tc.input, result, tc.expected)
		}
	}
}

func TestAnyToKeyString_Uint16(t *testing.T) {
	tests := []struct {
		input    uint16
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{65535, "65535"},
	}

	for _, tc := range tests {
		result := anyToKeyString(tc.input)
		if result != tc.expected {
			t.Errorf("anyToKeyString(%d) = %q, want %q", tc.input, result, tc.expected)
		}
	}
}

func TestAnyToKeyString_Uint8(t *testing.T) {
	tests := []struct {
		input    uint8
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{255, "255"},
	}

	for _, tc := range tests {
		result := anyToKeyString(tc.input)
		if result != tc.expected {
			t.Errorf("anyToKeyString(%d) = %q, want %q", tc.input, result, tc.expected)
		}
	}
}

func TestAnyToKeyString_String(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", ""},
		{"hello", "hello"},
		{"uuid-123-456", "uuid-123-456"},
		{"with spaces", "with spaces"},
	}

	for _, tc := range tests {
		result := anyToKeyString(tc.input)
		if result != tc.expected {
			t.Errorf("anyToKeyString(%q) = %q, want %q", tc.input, result, tc.expected)
		}
	}
}

func TestAnyToKeyString_ByteSlice(t *testing.T) {
	tests := []struct {
		input    []byte
		expected string
	}{
		{[]byte{}, ""},
		{[]byte("hello"), "hello"},
		{[]byte{0x68, 0x65, 0x6c, 0x6c, 0x6f}, "hello"},
	}

	for _, tc := range tests {
		result := anyToKeyString(tc.input)
		if result != tc.expected {
			t.Errorf("anyToKeyString(%v) = %q, want %q", tc.input, result, tc.expected)
		}
	}
}

func TestAnyToKeyString_FixedByteArray(t *testing.T) {
	// Test [16]byte (common for UUIDs stored as binary)
	var input [16]byte
	copy(input[:], "0123456789abcdef")
	expected := "0123456789abcdef"

	result := anyToKeyString(input)
	if result != expected {
		t.Errorf("anyToKeyString([16]byte) = %q, want %q", result, expected)
	}
}

func TestAnyToKeyString_UUID(t *testing.T) {
	// Test uuid.UUID which implements fmt.Stringer
	id := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	expected := "550e8400-e29b-41d4-a716-446655440000"

	result := anyToKeyString(id)
	if result != expected {
		t.Errorf("anyToKeyString(uuid.UUID) = %q, want %q", result, expected)
	}
}

// customStringer is a test type that implements fmt.Stringer
type customStringer struct {
	value string
}

func (c customStringer) String() string {
	return "custom:" + c.value
}

func TestAnyToKeyString_Stringer(t *testing.T) {
	input := customStringer{value: "test"}
	expected := "custom:test"

	result := anyToKeyString(input)
	if result != expected {
		t.Errorf("anyToKeyString(customStringer) = %q, want %q", result, expected)
	}
}

func TestAnyToKeyString_Fallback(t *testing.T) {
	// Test types that fall back to fmt.Sprintf
	tests := []struct {
		input    any
		expected string
	}{
		{float64(3.14), "3.14"},
		{float32(2.5), "2.5"},
		{true, "true"},
		{false, "false"},
		{struct{ X int }{X: 42}, "{42}"},
	}

	for _, tc := range tests {
		result := anyToKeyString(tc.input)
		if result != tc.expected {
			t.Errorf("anyToKeyString(%v) = %q, want %q", tc.input, result, tc.expected)
		}
	}
}

// ==================== Relation Interface Methods Tests ====================

type RelTestModel struct {
	ID   int `zorm:"primaryKey"`
	Name string
}

func (t RelTestModel) TableName() string { return "rel_test_models" }

func TestHasOne_RelationType(t *testing.T) {
	rel := HasOne[RelTestModel]{}
	if rel.RelationType() != RelationHasOne {
		t.Errorf("HasOne.RelationType() = %v, want %v", rel.RelationType(), RelationHasOne)
	}
}

func TestHasOne_NewRelated(t *testing.T) {
	rel := HasOne[RelTestModel]{}
	result := rel.NewRelated()

	ptr, ok := result.(*RelTestModel)
	if !ok {
		t.Errorf("HasOne.NewRelated() type = %T, want *RelTestModel", result)
	}
	if ptr == nil {
		t.Error("HasOne.NewRelated() returned nil")
	}
}

func TestHasOne_NewModel(t *testing.T) {
	rel := HasOne[RelTestModel]{}
	ctx := context.Background()
	var db *sql.DB // nil is acceptable for this test

	result := rel.NewModel(ctx, db)
	model, ok := result.(*Model[RelTestModel])
	if !ok {
		t.Errorf("HasOne.NewModel() type = %T, want *Model[RelTestModel]", result)
	}
	if model == nil {
		t.Error("HasOne.NewModel() returned nil")
	}
}

func TestHasMany_RelationType(t *testing.T) {
	rel := HasMany[RelTestModel]{}
	if rel.RelationType() != RelationHasMany {
		t.Errorf("HasMany.RelationType() = %v, want %v", rel.RelationType(), RelationHasMany)
	}
}

func TestHasMany_NewRelated(t *testing.T) {
	rel := HasMany[RelTestModel]{}
	result := rel.NewRelated()

	ptr, ok := result.(*RelTestModel)
	if !ok {
		t.Errorf("HasMany.NewRelated() type = %T, want *RelTestModel", result)
	}
	if ptr == nil {
		t.Error("HasMany.NewRelated() returned nil")
	}
}

func TestHasMany_NewModel(t *testing.T) {
	rel := HasMany[RelTestModel]{}
	ctx := context.Background()
	var db *sql.DB

	result := rel.NewModel(ctx, db)
	model, ok := result.(*Model[RelTestModel])
	if !ok {
		t.Errorf("HasMany.NewModel() type = %T, want *Model[RelTestModel]", result)
	}
	if model == nil {
		t.Error("HasMany.NewModel() returned nil")
	}
}

func TestBelongsTo_RelationType(t *testing.T) {
	rel := BelongsTo[RelTestModel]{}
	if rel.RelationType() != RelationBelongsTo {
		t.Errorf("BelongsTo.RelationType() = %v, want %v", rel.RelationType(), RelationBelongsTo)
	}
}

func TestBelongsTo_NewRelated(t *testing.T) {
	rel := BelongsTo[RelTestModel]{}
	result := rel.NewRelated()

	ptr, ok := result.(*RelTestModel)
	if !ok {
		t.Errorf("BelongsTo.NewRelated() type = %T, want *RelTestModel", result)
	}
	if ptr == nil {
		t.Error("BelongsTo.NewRelated() returned nil")
	}
}

func TestBelongsTo_NewModel(t *testing.T) {
	rel := BelongsTo[RelTestModel]{}
	ctx := context.Background()
	var db *sql.DB

	result := rel.NewModel(ctx, db)
	model, ok := result.(*Model[RelTestModel])
	if !ok {
		t.Errorf("BelongsTo.NewModel() type = %T, want *Model[RelTestModel]", result)
	}
	if model == nil {
		t.Error("BelongsTo.NewModel() returned nil")
	}
}

func TestBelongsToMany_RelationType(t *testing.T) {
	rel := BelongsToMany[RelTestModel]{}
	if rel.RelationType() != RelationBelongsToMany {
		t.Errorf("BelongsToMany.RelationType() = %v, want %v", rel.RelationType(), RelationBelongsToMany)
	}
}

func TestBelongsToMany_NewRelated(t *testing.T) {
	rel := BelongsToMany[RelTestModel]{}
	result := rel.NewRelated()

	ptr, ok := result.(*RelTestModel)
	if !ok {
		t.Errorf("BelongsToMany.NewRelated() type = %T, want *RelTestModel", result)
	}
	if ptr == nil {
		t.Error("BelongsToMany.NewRelated() returned nil")
	}
}

func TestBelongsToMany_NewModel(t *testing.T) {
	rel := BelongsToMany[RelTestModel]{}
	ctx := context.Background()
	var db *sql.DB

	result := rel.NewModel(ctx, db)
	model, ok := result.(*Model[RelTestModel])
	if !ok {
		t.Errorf("BelongsToMany.NewModel() type = %T, want *Model[RelTestModel]", result)
	}
	if model == nil {
		t.Error("BelongsToMany.NewModel() returned nil")
	}
}

func TestMorphTo_RelationType(t *testing.T) {
	rel := MorphTo[any]{}
	if rel.RelationType() != RelationMorphTo {
		t.Errorf("MorphTo.RelationType() = %v, want %v", rel.RelationType(), RelationMorphTo)
	}
}

func TestMorphTo_NewRelated(t *testing.T) {
	rel := MorphTo[any]{}
	result := rel.NewRelated()

	// MorphTo.NewRelated() returns nil because the related type is dynamic
	if result != nil {
		t.Errorf("MorphTo.NewRelated() = %v, want nil", result)
	}
}

func TestMorphTo_NewModel(t *testing.T) {
	rel := MorphTo[any]{}
	ctx := context.Background()
	var db *sql.DB

	result := rel.NewModel(ctx, db)
	// MorphTo.NewModel() returns nil because the model type is dynamic
	if result != nil {
		t.Errorf("MorphTo.NewModel() = %v, want nil", result)
	}
}

func TestMorphOne_RelationType(t *testing.T) {
	rel := MorphOne[RelTestModel]{}
	if rel.RelationType() != RelationMorphOne {
		t.Errorf("MorphOne.RelationType() = %v, want %v", rel.RelationType(), RelationMorphOne)
	}
}

func TestMorphOne_NewRelated(t *testing.T) {
	rel := MorphOne[RelTestModel]{}
	result := rel.NewRelated()

	ptr, ok := result.(*RelTestModel)
	if !ok {
		t.Errorf("MorphOne.NewRelated() type = %T, want *RelTestModel", result)
	}
	if ptr == nil {
		t.Error("MorphOne.NewRelated() returned nil")
	}
}

func TestMorphOne_NewModel(t *testing.T) {
	rel := MorphOne[RelTestModel]{}
	ctx := context.Background()
	var db *sql.DB

	result := rel.NewModel(ctx, db)
	model, ok := result.(*Model[RelTestModel])
	if !ok {
		t.Errorf("MorphOne.NewModel() type = %T, want *Model[RelTestModel]", result)
	}
	if model == nil {
		t.Error("MorphOne.NewModel() returned nil")
	}
}

func TestMorphMany_RelationType(t *testing.T) {
	rel := MorphMany[RelTestModel]{}
	if rel.RelationType() != RelationMorphMany {
		t.Errorf("MorphMany.RelationType() = %v, want %v", rel.RelationType(), RelationMorphMany)
	}
}

func TestMorphMany_NewRelated(t *testing.T) {
	rel := MorphMany[RelTestModel]{}
	result := rel.NewRelated()

	ptr, ok := result.(*RelTestModel)
	if !ok {
		t.Errorf("MorphMany.NewRelated() type = %T, want *RelTestModel", result)
	}
	if ptr == nil {
		t.Error("MorphMany.NewRelated() returned nil")
	}
}

func TestMorphMany_NewModel(t *testing.T) {
	rel := MorphMany[RelTestModel]{}
	ctx := context.Background()
	var db *sql.DB

	result := rel.NewModel(ctx, db)
	model, ok := result.(*Model[RelTestModel])
	if !ok {
		t.Errorf("MorphMany.NewModel() type = %T, want *Model[RelTestModel]", result)
	}
	if model == nil {
		t.Error("MorphMany.NewModel() returned nil")
	}
}

// ==================== GetOverrideTable Tests ====================

func TestHasOne_GetOverrideTable(t *testing.T) {
	tests := []struct {
		name     string
		rel      HasOne[RelTestModel]
		expected string
	}{
		{"empty table", HasOne[RelTestModel]{}, ""},
		{"custom table", HasOne[RelTestModel]{Table: "custom_table"}, "custom_table"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.rel.GetOverrideTable()
			if result != tc.expected {
				t.Errorf("GetOverrideTable() = %q, want %q", result, tc.expected)
			}
		})
	}
}

func TestHasMany_GetOverrideTable(t *testing.T) {
	tests := []struct {
		name     string
		rel      HasMany[RelTestModel]
		expected string
	}{
		{"empty table", HasMany[RelTestModel]{}, ""},
		{"custom table", HasMany[RelTestModel]{Table: "posts_archive"}, "posts_archive"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.rel.GetOverrideTable()
			if result != tc.expected {
				t.Errorf("GetOverrideTable() = %q, want %q", result, tc.expected)
			}
		})
	}
}

func TestBelongsTo_GetOverrideTable(t *testing.T) {
	tests := []struct {
		name     string
		rel      BelongsTo[RelTestModel]
		expected string
	}{
		{"empty table", BelongsTo[RelTestModel]{}, ""},
		{"custom table", BelongsTo[RelTestModel]{Table: "parent_table"}, "parent_table"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.rel.GetOverrideTable()
			if result != tc.expected {
				t.Errorf("GetOverrideTable() = %q, want %q", result, tc.expected)
			}
		})
	}
}

func TestMorphOne_GetOverrideTable(t *testing.T) {
	tests := []struct {
		name     string
		rel      MorphOne[RelTestModel]
		expected string
	}{
		{"empty table", MorphOne[RelTestModel]{}, ""},
		{"custom table", MorphOne[RelTestModel]{Table: "images_v2"}, "images_v2"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.rel.GetOverrideTable()
			if result != tc.expected {
				t.Errorf("GetOverrideTable() = %q, want %q", result, tc.expected)
			}
		})
	}
}

func TestMorphMany_GetOverrideTable(t *testing.T) {
	tests := []struct {
		name     string
		rel      MorphMany[RelTestModel]
		expected string
	}{
		{"empty table", MorphMany[RelTestModel]{}, ""},
		{"custom table", MorphMany[RelTestModel]{Table: "comments_v2"}, "comments_v2"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.rel.GetOverrideTable()
			if result != tc.expected {
				t.Errorf("GetOverrideTable() = %q, want %q", result, tc.expected)
			}
		})
	}
}

// ==================== isZero Tests ====================

func TestIsZero_Nil(t *testing.T) {
	if !isZero(nil) {
		t.Error("isZero(nil) = false, want true")
	}
}

func TestIsZero_ZeroValues(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{"int zero", 0},
		{"int64 zero", int64(0)},
		{"string empty", ""},
		{"bool false", false},
		{"float zero", 0.0},
		{"uuid nil", uuid.Nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if !isZero(tc.value) {
				t.Errorf("isZero(%v) = false, want true", tc.value)
			}
		})
	}
}

func TestIsZero_NonZeroValues(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{"int non-zero", 1},
		{"int64 non-zero", int64(42)},
		{"string non-empty", "hello"},
		{"bool true", true},
		{"float non-zero", 3.14},
		{"uuid valid", uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if isZero(tc.value) {
				t.Errorf("isZero(%v) = true, want false", tc.value)
			}
		})
	}
}

// ==================== TableOverrider Interface Verification ====================

func TestTableOverriderInterface(t *testing.T) {
	// Verify that all relation types implement TableOverrider
	var _ TableOverrider = HasOne[RelTestModel]{}
	var _ TableOverrider = HasMany[RelTestModel]{}
	var _ TableOverrider = BelongsTo[RelTestModel]{}
	var _ TableOverrider = MorphOne[RelTestModel]{}
	var _ TableOverrider = MorphMany[RelTestModel]{}
}

// ==================== Relation Interface Verification ====================

func TestRelationInterface(t *testing.T) {
	// Verify that all relation types implement Relation interface
	var _ Relation = HasOne[RelTestModel]{}
	var _ Relation = HasMany[RelTestModel]{}
	var _ Relation = BelongsTo[RelTestModel]{}
	var _ Relation = BelongsToMany[RelTestModel]{}
	var _ Relation = MorphTo[any]{}
	var _ Relation = MorphOne[RelTestModel]{}
	var _ Relation = MorphMany[RelTestModel]{}
}

// ==================== RelationType Constants Tests ====================

func TestRelationTypeConstants(t *testing.T) {
	tests := []struct {
		relType  RelationType
		expected string
	}{
		{RelationHasOne, "HasOne"},
		{RelationHasMany, "HasMany"},
		{RelationBelongsTo, "BelongsTo"},
		{RelationBelongsToMany, "BelongsToMany"},
		{RelationMorphTo, "MorphTo"},
		{RelationMorphOne, "MorphOne"},
		{RelationMorphMany, "MorphMany"},
	}

	for _, tc := range tests {
		if string(tc.relType) != tc.expected {
			t.Errorf("RelationType constant = %q, want %q", tc.relType, tc.expected)
		}
	}
}

// ==================== Edge Cases ====================

func TestAnyToKeyString_NilUUID(t *testing.T) {
	// uuid.Nil implements fmt.Stringer and returns "00000000-0000-0000-0000-000000000000"
	result := anyToKeyString(uuid.Nil)
	expected := "00000000-0000-0000-0000-000000000000"
	if result != expected {
		t.Errorf("anyToKeyString(uuid.Nil) = %q, want %q", result, expected)
	}
}

func TestAnyToKeyString_NegativeInts(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{"negative int64", int64(-9223372036854775808), "-9223372036854775808"},
		{"negative int32", int32(-2147483648), "-2147483648"},
		{"negative int16", int16(-32768), "-32768"},
		{"negative int8", int8(-128), "-128"},
		{"negative int", int(-999999), "-999999"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := anyToKeyString(tc.input)
			if result != tc.expected {
				t.Errorf("anyToKeyString(%v) = %q, want %q", tc.input, result, tc.expected)
			}
		})
	}
}

// customNonStringer is a test type that does NOT implement fmt.Stringer
type customNonStringer struct {
	X int
	Y string
}

func TestAnyToKeyString_CustomStruct(t *testing.T) {
	input := customNonStringer{X: 1, Y: "test"}
	// fmt.Sprintf("%v", input) returns "{1 test}"
	expected := fmt.Sprintf("%v", input)

	result := anyToKeyString(input)
	if result != expected {
		t.Errorf("anyToKeyString(%+v) = %q, want %q", input, result, expected)
	}
}
