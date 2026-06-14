package zorm

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestToSnakeCase(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"UserID", "user_id"},
		{"HTTPClient", "http_client"},
		{"Simple", "simple"},
		{"myField", "my_field"},
		{"MyField", "my_field"},
		{"ID", "id"},
		{"URL", "url"},
		{"HTTPServerURL", "http_server_url"},
		{"A1", "a_1"},
		{"Field1", "field_1"},
	}

	for _, test := range tests {
		if got := ToSnakeCase(test.input); got != test.expected {
			t.Errorf("ToSnakeCase(%q) = %q; want %q", test.input, got, test.expected)
		}
	}
}

type Embedded struct {
	EmbeddedField string
}

type TestModel struct {
	ID   int
	Name string
	Age  int `zorm:"column:user_age"`
	Embedded
	Ignored string `zorm:"-"`
}

func TestParseModel_Embedded(t *testing.T) {
	info := ParseModel[TestModel]()

	if info.TableName != "test_models" {
		t.Errorf("expected table name test_models, got %s", info.TableName)
	}

	if info.PrimaryKey != "id" {
		t.Errorf("expected pk id, got %s", info.PrimaryKey)
	}

	// Check fields
	if _, ok := info.Fields["Name"]; !ok {
		t.Error("missing field Name")
	}
	if _, ok := info.Fields["EmbeddedField"]; !ok {
		t.Error("missing embedded field EmbeddedField")
	}
	if _, ok := info.Fields["Ignored"]; ok {
		t.Error("field Ignored should be ignored")
	}

	// Check column mapping
	if f, ok := info.Columns["user_age"]; !ok || f.Name != "Age" {
		t.Error("custom column mapping failed")
	}
	if f, ok := info.Columns["embedded_field"]; !ok || f.Name != "EmbeddedField" {
		t.Error("embedded field column mapping failed")
	}
}

// RelatedModel is used as a relation target
type RelatedModel struct {
	ID   int
	Name string
}

// ModelWithRelations tests that relation fields are excluded from columns
type ModelWithRelations struct {
	ID          int
	Name        string
	Related     *RelatedModel   // Should be skipped (pointer to struct)
	RelatedMany []*RelatedModel // Should be skipped (slice of pointers to structs)
}

func TestParseModel_SkipsRelationFields(t *testing.T) {
	info := ParseModel[ModelWithRelations]()

	// Regular fields should be present
	if _, ok := info.Fields["ID"]; !ok {
		t.Error("missing field ID")
	}
	if _, ok := info.Fields["Name"]; !ok {
		t.Error("missing field Name")
	}

	// Relation fields should be excluded
	if _, ok := info.Fields["Related"]; ok {
		t.Error("field Related (pointer to struct) should be skipped as relation")
	}
	if _, ok := info.Fields["RelatedMany"]; ok {
		t.Error("field RelatedMany (slice of structs) should be skipped as relation")
	}

	// Columns should also not contain relation fields
	if _, ok := info.Columns["related"]; ok {
		t.Error("column related should not exist")
	}
	if _, ok := info.Columns["related_many"]; ok {
		t.Error("column related_many should not exist")
	}
}

func TestFillStruct(t *testing.T) {
	m := &TestModel{}
	data := map[string]any{
		"id":             int64(123), // int64 -> int
		"name":           "Alice",
		"user_age":       "30",                     // string -> int
		"embedded_field": []byte("Embedded Value"), // []byte -> string
	}

	if err := fillStruct(m, data); err != nil {
		t.Fatalf("fillStruct failed: %v", err)
	}

	if m.ID != 123 {
		t.Errorf("expected ID 123, got %d", m.ID)
	}
	if m.Name != "Alice" {
		t.Errorf("expected Name Alice, got %q", m.Name)
	}
	if m.Age != 30 {
		t.Errorf("expected Age 30, got %d", m.Age)
	}
	if m.EmbeddedField != "Embedded Value" {
		t.Errorf("expected EmbeddedField 'Embedded Value', got %q", m.EmbeddedField)
	}
}

// Mock Scanner
type CustomScanner struct {
	Value string
}

func (c *CustomScanner) Scan(src any) error {
	if s, ok := src.(string); ok {
		c.Value = s
		return nil
	}
	return nil
}

type ScannerModel struct {
	Custom CustomScanner
}

func TestFillStruct_Scanner(t *testing.T) {
	m := &ScannerModel{}
	data := map[string]any{
		"custom": "scanned_value",
	}

	if err := fillStruct(m, data); err != nil {
		t.Fatalf("fillStruct failed: %v", err)
	}

	if m.Custom.Value != "scanned_value" {
		t.Errorf("expected Custom.Value 'scanned_value', got %q", m.Custom.Value)
	}
}

type TypeModel struct {
	Uint8   uint8
	Uint64  uint64
	Float32 float32
	Float64 float64
	Bool    bool
	PtrInt  *int
}

func TestFillStruct_AllTypes(t *testing.T) {
	m := &TypeModel{}
	data := map[string]any{
		"uint_8":   "255",
		"uint_64":  int64(1000),
		"float_32": "12.34",
		"float_64": 123.456,
		"bool":     "true",
		"ptr_int":  int64(42),
	}

	if err := fillStruct(m, data); err != nil {
		t.Fatalf("fillStruct failed: %v", err)
	}

	if m.Uint8 != 255 {
		t.Errorf("expected Uint8 255, got %d", m.Uint8)
	}
	if m.Uint64 != 1000 {
		t.Errorf("expected Uint64 1000, got %d", m.Uint64)
	}
	if m.Float32 < 12.33 || m.Float32 > 12.35 {
		t.Errorf("expected Float32 ~12.34, got %f", m.Float32)
	}
	if m.Float64 != 123.456 {
		t.Errorf("expected Float64 123.456, got %f", m.Float64)
	}
	if !m.Bool {
		t.Error("expected Bool true, got false")
	}
	if m.PtrInt == nil || *m.PtrInt != 42 {
		t.Errorf("expected PtrInt 42, got %v", m.PtrInt)
	}

	// Test bool with int
	data2 := map[string]any{"bool": 0}
	fillStruct(m, data2)
	if m.Bool {
		t.Error("expected Bool false for 0")
	}

	data3 := map[string]any{"bool": 1}
	fillStruct(m, data3)
	if !m.Bool {
		t.Error("expected Bool true for 1")
	}
}

func TestSetFieldValue_NilHandling(t *testing.T) {
	m := &TypeModel{
		PtrInt: new(int),
	}
	*m.PtrInt = 10

	// Set pointer to nil
	data := map[string]any{
		"ptr_int": nil,
	}
	if err := fillStruct(m, data); err != nil {
		t.Errorf("fillStruct failed: %v", err)
	}
	if m.PtrInt != nil {
		t.Error("expected PtrInt to be nil")
	}
}

func TestSetField_Exhaustive(t *testing.T) {
	type ExhaustiveModel struct {
		Int   int64
		Uint  uint64
		Float float64
		Bool  bool
	}
	info := ParseModelType(reflect.TypeOf(ExhaustiveModel{}))

	t.Run("setIntField", func(t *testing.T) {
		targets := []any{
			int(10), int8(10), int16(10), int32(10), int64(10),
			float32(10.5), float64(10.9), "10", []byte("10"),
		}
		for _, val := range targets {
			m := &ExhaustiveModel{}
			f := reflect.ValueOf(m).Elem().FieldByName("Int")
			if err := setIntField(f, val); err != nil {
				t.Errorf("setIntField failed for %T: %v", val, err)
			}
			if m.Int != 10 {
				t.Errorf("expected 10 from %T, got %d", val, m.Int)
			}
		}
	})

	t.Run("setUintField", func(t *testing.T) {
		targets := []any{
			int(10), int64(10), uint64(10), float64(10.5), "10", []byte("10"),
		}
		for _, val := range targets {
			m := &ExhaustiveModel{}
			f := reflect.ValueOf(m).Elem().FieldByName("Uint")
			if err := setUintField(f, val); err != nil {
				t.Errorf("setUintField failed for %T: %v", val, err)
			}
			if m.Uint != 10 {
				t.Errorf("expected 10 from %T, got %d", val, m.Uint)
			}
		}
	})

	t.Run("setFloatField", func(t *testing.T) {
		tests := []struct {
			input any
			want  float64
		}{
			{float32(10.5), 10.5}, {float64(10.5), 10.5},
			{int(10), 10.0}, {int64(10), 10.0},
			{"10.5", 10.5}, {[]byte("10.5"), 10.5},
		}
		for _, tc := range tests {
			m := &ExhaustiveModel{}
			f := reflect.ValueOf(m).Elem().FieldByName("Float")
			if err := setFloatField(f, tc.input); err != nil {
				t.Errorf("setFloatField failed for %T: %v", tc.input, err)
			}
			if m.Float != tc.want {
				t.Errorf("expected %f from %T, got %f", tc.want, tc.input, m.Float)
			}
		}
	})

	t.Run("error_cases", func(t *testing.T) {
		m := &ExhaustiveModel{}
		fInt := reflect.ValueOf(m).Elem().FieldByName("Int")
		fUint := reflect.ValueOf(m).Elem().FieldByName("Uint")
		fFloat := reflect.ValueOf(m).Elem().FieldByName("Float")
		fBool := reflect.ValueOf(m).Elem().FieldByName("Bool")

		if err := setIntField(fInt, "abc"); err == nil {
			t.Error("expected error for invalid int string")
		}
		if err := setIntField(fInt, true); err == nil {
			t.Error("expected error for bool to int")
		}
		if err := setUintField(fUint, "abc"); err == nil {
			t.Error("expected error for invalid uint string")
		}
		if err := setUintField(fUint, -1); err == nil {
			// -1 as int to uint64 works in Go bitwise, but let's see if zorm handles it.
			// setUintField does uint64(v), which for -1 is huge.
			// But zorm doesn't check sign for int types to uint conversion.
		}
		if err := setUintField(fUint, true); err == nil {
			t.Error("expected error for bool to uint")
		}
		if err := setFloatField(fFloat, "abc"); err == nil {
			t.Error("expected error for invalid float string")
		}
		if err := setFloatField(fFloat, true); err == nil {
			t.Error("expected error for bool to float")
		}
		if err := setBoolField(fBool, "abc"); err == nil {
			t.Error("expected error for invalid bool string")
		}
		if err := setBoolField(fBool, 1.5); err == nil {
			t.Error("expected error for float to bool")
		}
	})

	t.Run("setBoolField", func(t *testing.T) {
		targets := []struct {
			input any
			want  bool
		}{
			{true, true}, {false, false},
			{int(1), true}, {int(0), false},
			{int64(1), true}, {int64(0), false},
			{"true", true}, {"0", false},
			{[]byte("true"), true}, {[]byte("false"), false},
		}
		for _, tc := range targets {
			m := &ExhaustiveModel{}
			f := reflect.ValueOf(m).Elem().FieldByName("Bool")
			if err := setBoolField(f, tc.input); err != nil {
				t.Errorf("setBoolField failed for %T: %v", tc.input, err)
			}
			if m.Bool != tc.want {
				t.Errorf("expected %v from %T, got %v", tc.want, tc.input, m.Bool)
			}
		}
	})

	_ = info // Avoid unused
}

// bareTagModel exercises the new bare-form tag shorthand.
type bareTagModel struct {
	ID       int
	Name     string `zorm:"full_name"`
	NickName string `zorm:"alias"`
}

func TestParseModel_BareFormTag(t *testing.T) {
	info := ParseModel[bareTagModel]()

	if f, ok := info.Columns["full_name"]; !ok || f.Name != "Name" {
		t.Errorf("expected bare-form column full_name -> Name, got %+v (ok=%v)", f, ok)
	}
	if f, ok := info.Columns["alias"]; !ok || f.Name != "NickName" {
		t.Errorf("expected bare-form column alias -> NickName, got %+v (ok=%v)", f, ok)
	}
	// Default snake_case is unchanged when no tag is given.
	if _, ok := info.Columns["id"]; !ok {
		t.Error("expected default column id for ID field")
	}
	// Ensure the original default column (snake_case of field name) is NOT
	// also registered when the bare tag overrides it.
	if _, ok := info.Columns["name"]; ok {
		t.Error("bare tag full_name should replace default column name, not add it")
	}
}

// primaryKeyTagModel exercises the new `primaryKey` keyword on a non-ID field.
type primaryKeyTagModel struct {
	UUID string `zorm:"primaryKey"`
	Name string
}

func TestParseModel_PrimaryKeyKeyword(t *testing.T) {
	info := ParseModel[primaryKeyTagModel]()

	if info.PrimaryKey != "uuid" {
		t.Errorf("expected primary key uuid, got %s", info.PrimaryKey)
	}
	f, ok := info.Columns["uuid"]
	if !ok {
		t.Fatal("missing column uuid")
	}
	if !f.IsPrimary {
		t.Error("UUID should be marked primary")
	}
	// `primaryKey` implies auto-increment only for integer kinds.
	//  A string PK like a UUID must NOT be flagged auto, otherwise
	// caller-supplied values would be dropped on insert.
	if f.IsAuto {
		t.Error("primaryKey shorthand on a string PK must NOT imply auto-increment")
	}
}

// mixedTagModel mixes a bare-form column name with a keyword flag.
type mixedTagModel struct {
	ID  int
	Foo int `zorm:"primary;widget_id"`
}

func TestParseModel_MixedBareAndKeyword(t *testing.T) {
	info := ParseModel[mixedTagModel]()

	if info.PrimaryKey != "widget_id" {
		t.Errorf("expected primary key widget_id, got %s", info.PrimaryKey)
	}
	f, ok := info.Columns["widget_id"]
	if !ok {
		t.Fatal("missing column widget_id")
	}
	if !f.IsPrimary {
		t.Error("Foo should be marked primary")
	}
	if f.IsAuto {
		t.Error("Foo should NOT be auto (no auto/primaryKey keyword present)")
	}
}

// longFormTagModel verifies that the existing long-form tags still work
// untouched after the parser extension.
type longFormTagModel struct {
	ID    int64  `zorm:"column:id;primary;auto"`
	Email string `zorm:"column:email_address"`
}

func TestParseModel_LongFormStillWorks(t *testing.T) {
	info := ParseModel[longFormTagModel]()

	if info.PrimaryKey != "id" {
		t.Errorf("expected primary key id, got %s", info.PrimaryKey)
	}
	pk, ok := info.Columns["id"]
	if !ok {
		t.Fatal("missing column id")
	}
	if !pk.IsPrimary || !pk.IsAuto {
		t.Errorf("expected id to be primary+auto, got primary=%v auto=%v", pk.IsPrimary, pk.IsAuto)
	}
	if _, ok := info.Columns["email_address"]; !ok {
		t.Error("missing column email_address")
	}
}

// orderIndepModel — bare token appears BEFORE the keyword. Result must match
// the keyword-first ordering covered in TestParseModel_MixedBareAndKeyword.
type orderIndepModel struct {
	ID  int
	Foo int `zorm:"widget_id;primary"`
}

func TestParseModel_BareAndKeywordOrderIndependent(t *testing.T) {
	info := ParseModel[orderIndepModel]()

	if info.PrimaryKey != "widget_id" {
		t.Errorf("expected primary key widget_id, got %s", info.PrimaryKey)
	}
	f, ok := info.Columns["widget_id"]
	if !ok {
		t.Fatal("missing column widget_id")
	}
	if !f.IsPrimary {
		t.Error("Foo should be marked primary regardless of token order")
	}
	if f.IsAuto {
		t.Error("Foo should NOT be auto (only 'primary' keyword, not 'auto' or 'primaryKey')")
	}
}

// forwardCompatModel — an unknown `key:value` pair must remain a no-op so the
// tag vocabulary can grow later without churning consumers. Only the bare,
// no-colon form is treated as a column override.
type forwardCompatModel struct {
	ID   int
	Name string `zorm:"unknown:something;primary"`
}

func TestParseModel_UnknownKeyValueIsNoOp(t *testing.T) {
	info := ParseModel[forwardCompatModel]()

	// Column must stay as default snake_case of the field name, NOT 'unknown'.
	if _, ok := info.Columns["name"]; !ok {
		t.Error("expected default column name to survive unknown key:value tag")
	}
	if _, ok := info.Columns["unknown"]; ok {
		t.Error("unknown key:value pair must not set the column name")
	}
	if _, ok := info.Columns["something"]; ok {
		t.Error("value half of unknown pair must not become a column")
	}
	// Sibling keyword in the same tag must still apply.
	if info.PrimaryKey != "name" {
		t.Errorf("expected primary key name (set by sibling 'primary'), got %s", info.PrimaryKey)
	}
}

// reservedEscapeModel — column literally named `primary` collides with the
// reserved keyword. Long-form `column:primary` must win and override the
// keyword interpretation.
type reservedEscapeModel struct {
	ID    int
	Field string `zorm:"column:primary"`
}

func TestParseModel_ReservedKeywordEscapeViaColumnPrefix(t *testing.T) {
	info := ParseModel[reservedEscapeModel]()

	f, ok := info.Columns["primary"]
	if !ok {
		t.Fatal("expected column literally named 'primary' from column: escape")
	}
	if f.Name != "Field" {
		t.Errorf("expected Field->primary mapping, got Name=%s", f.Name)
	}
	if f.IsPrimary {
		t.Error("column:primary must NOT mark the field as primary key")
	}
	if info.PrimaryKey != "id" {
		t.Errorf("primary key should still be id, got %s", info.PrimaryKey)
	}
}

// collisionInner — embedded; carries a bare-form tag that maps to a column
// also produced by another sibling's default snake_case. Parser must accept
// both Fields entries; Columns is a map so the last-parsed FieldInfo wins
// (documented behavior — caller must avoid the collision).
type CollisionInner struct {
	FullName string // default column: full_name
}
type collisionModel struct {
	ID    int
	Other string `zorm:"full_name"` // bare-form collides with FullName's default
	CollisionInner
}

func TestParseModel_BareTagCollisionWithDefaultSnakeCase(t *testing.T) {
	info := ParseModel[collisionModel]()

	// Both Field entries must exist — Fields is keyed by Go field name.
	if _, ok := info.Fields["Other"]; !ok {
		t.Error("Field 'Other' should still be registered")
	}
	if _, ok := info.Fields["FullName"]; !ok {
		t.Error("Embedded field 'FullName' should still be registered")
	}

	// Columns is map[string]*FieldInfo — collision means one wins. Document
	// the winner so callers see deterministic behavior. parseFields walks
	// top-level fields then embedded; the embedded FullName is parsed last
	// because the embedded struct sits AFTER `Other` in the outer struct's
	// declaration order, so FullName's default snake_case is the final
	// write to Columns["full_name"].
	col, ok := info.Columns["full_name"]
	if !ok {
		t.Fatal("missing column full_name")
	}
	if col.Name != "FullName" {
		t.Errorf("expected late-parsed FullName to win the column collision, got %s", col.Name)
	}

	// Sanity: id default still present.
	if _, ok := info.Columns["id"]; !ok {
		t.Error("missing default id column")
	}
}

// embeddedBareTagOuter — bare-form tag on a field inside an embedded struct
// must still take effect when reached via parseFields' recursive walk.
type EmbeddedBareTagInner struct {
	Email string `zorm:"contact_email"`
}
type embeddedBareTagOuter struct {
	ID int
	EmbeddedBareTagInner
	Note string `zorm:"memo"`
}

func TestParseModel_BareTagInsideEmbeddedStruct(t *testing.T) {
	info := ParseModel[embeddedBareTagOuter]()

	f, ok := info.Columns["contact_email"]
	if !ok {
		t.Fatal("expected embedded field's bare-tag column contact_email to be registered")
	}
	if f.Name != "Email" {
		t.Errorf("expected Email field to back column contact_email, got %s", f.Name)
	}
	// Field index must address the embedded path (len > 1) so reflection can
	// reach the value at runtime — otherwise scans/inserts would panic.
	if len(f.Index) < 2 {
		t.Errorf("expected nested field index path for embedded field, got %v", f.Index)
	}

	// Outer-level bare tag also lands.
	if _, ok := info.Columns["memo"]; !ok {
		t.Error("missing bare-tag column memo on outer field")
	}

	// Default snake_case for the embedded field's struct-style name MUST NOT
	// also land — the tag override replaces it.
	if _, ok := info.Columns["email"]; ok {
		t.Error("embedded Email should not also register as the default 'email' column when a bare tag overrides it")
	}
}

// crudBareTagModel — end-to-end test: bare-form tag must propagate through
// the executor (INSERT column list, UPDATE SET clause, SELECT scan), not just
// the parsed ModelInfo. Uses a real SQLite table whose column names match
// the bare-form tags rather than the Go field names.
type crudBareTagModel struct {
	ID       int    `zorm:"primaryKey"`
	UserName string `zorm:"login"`   // DB column: login (NOT user_name)
	Bio      string `zorm:"profile"` // DB column: profile
}

func (crudBareTagModel) TableName() string { return "bare_crud" }

func TestParseModel_BareTagEndToEndCRUD(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE bare_crud (id INTEGER PRIMARY KEY, login TEXT, profile TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	ctx := context.Background()

	// Create — INSERT must use `login` and `profile`, NOT `user_name`/`bio`.
	row := &crudBareTagModel{UserName: "alice", Bio: "first bio"}
	if err := New[crudBareTagModel]().SetDB(db).Create(ctx, row); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if row.ID == 0 {
		t.Fatal("expected RETURNING id to populate row.ID")
	}

	// First — SELECT scan must map login/profile back into the Go fields.
	got, err := New[crudBareTagModel]().SetDB(db).Where("id", row.ID).First(ctx)
	if err != nil {
		t.Fatalf("First: %v", err)
	}
	if got.UserName != "alice" || got.Bio != "first bio" {
		t.Errorf("scan mismatch: got %+v", got)
	}

	// Update — SET clause must reference login/profile.
	got.UserName = "alice2"
	got.Bio = "second bio"
	if err := New[crudBareTagModel]().SetDB(db).Update(ctx, got); err != nil {
		t.Fatalf("Update: %v", err)
	}

	// Verify with a raw SQL query against the actual DB column names so the
	// test fails loudly if the executor accidentally generated user_name/bio.
	var login, profile string
	if err := db.QueryRow(`SELECT login, profile FROM bare_crud WHERE id = ?`, row.ID).Scan(&login, &profile); err != nil {
		t.Fatalf("raw verify: %v", err)
	}
	if login != "alice2" || profile != "second bio" {
		t.Errorf("DB row mismatch: login=%q profile=%q", login, profile)
	}
}
