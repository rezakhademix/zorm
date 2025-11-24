package zorm

import (
	"testing"
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
