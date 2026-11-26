package zorm

import (
	"reflect"
	"testing"
)

type ordinaryBytes []byte

func TestSetFieldValue_ByteSlicesToStrings(t *testing.T) {
	inputs := []struct {
		name  string
		value any
		want  string
	}{
		{"standard bytes", []byte("abc"), "abc"},
		{"named slice", ordinaryBytes("abc"), "abc"},
		{"named elements", []byteAlias{'a', 'b', 'c'}, "abc"},
		{"named slice and elements", byteSliceAlias{'a', 'b', 'c'}, "abc"},
		{"empty", byteSliceAlias{}, ""},
		{"nil", byteSliceAlias(nil), ""},
		{"UTF8", byteSliceAlias([]byteAlias{0xd8, 0xb3}), "س"},
		{"raw bytes", byteSliceAlias{0, 0xff, 'a'}, "\x00\xffa"},
	}
	outputs := []struct {
		name string
		typ  reflect.Type
	}{
		{"string", reflect.TypeFor[string]()},
		{"named string", reflect.TypeFor[textAlias]()},
		{"string pointer", reflect.TypeFor[*string]()},
		{"named string pointer", reflect.TypeFor[*textAlias]()},
	}
	for _, input := range inputs {
		for _, output := range outputs {
			t.Run(input.name+"/"+output.name, func(t *testing.T) {
				field := reflect.New(output.typ).Elem()
				if err := setFieldValue(field, input.value); err != nil {
					t.Fatal(err)
				}
				if field.Kind() == reflect.Pointer {
					if field.IsNil() {
						t.Fatal("typed byte slice must produce a string value")
					}
					field = field.Elem()
				}
				if got := field.String(); got != input.want {
					t.Errorf("conversion got %q, want %q", got, input.want)
				}
			})
		}
	}
}
