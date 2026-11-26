package zorm

import (
	"math"
	"reflect"
	"testing"
)

type byteAlias uint8
type byteSliceAlias []byteAlias
type textAlias string

func TestSchemaGuards_VersionHelpersAndNilConversions(t *testing.T) {
	if maxVersionForKind(reflect.String) != math.MaxInt64 || toInt64Version(reflect.ValueOf("invalid")) != 0 {
		t.Error("unsupported version helpers returned unsafe defaults")
	}
	setInt64Version(reflect.ValueOf(3), 4) // Read-only reflect.Value must remain untouched.
	value := 7
	if err := setFieldValue(reflect.ValueOf(&value).Elem(), nil); err != nil || value != 7 {
		t.Errorf("nil partial update erased scalar: %d %v", value, err)
	}
	for _, input := range []any{[]byte("abc"), []byteAlias{'a', 'b', 'c'}, byteSliceAlias{'a', 'b', 'c'}} {
		var output textAlias
		if err := setFieldValue(reflect.ValueOf(&output).Elem(), input); err != nil || output != "abc" {
			t.Errorf("byte-to-text conversion failed: %T %q %v", input, output, err)
		}
	}
	for _, item := range []struct{ input, want string }{{"2A", "2_a"}, {"A12", "a_12"}} {
		if got := ToSnakeCase(item.input); got != item.want {
			t.Errorf("ToSnakeCase(%q)=%q want %q", item.input, got, item.want)
		}
	}
}

func TestSchemaGuards_InvalidBareTagPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("malformed bare column tag was accepted")
		}
	}()
	ParseModel[struct {
		Name string `zorm:"bad,column"`
	}]()
}
