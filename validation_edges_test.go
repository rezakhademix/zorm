package zorm

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestFieldConversion_RejectsInvalidValues(t *testing.T) {
	for _, target := range []any{int64(7), uint64(7), float64(7), true} {
		for _, input := range []any{[]byte("invalid"), "invalid", map[string]int{"value": 1}} {
			field := reflect.New(reflect.TypeOf(target)).Elem()
			field.Set(reflect.ValueOf(target))
			if err := setFieldValue(field, input); err == nil {
				t.Errorf("conversion from %T to %T should fail", input, target)
			}
			if !reflect.DeepEqual(field.Interface(), target) {
				t.Errorf("invalid conversion changed %T: got %v want %v", target, field.Interface(), target)
			}
		}
	}
	var unsupported struct{ Value int }
	if err := setFieldValue(reflect.ValueOf(&unsupported).Elem(), "invalid"); err == nil {
		t.Error("unsupported struct conversion accepted")
	}
	var text string
	if err := setFieldValue(reflect.ValueOf(&text).Elem(), map[string]int{"value": 1}); err != nil || text != "map[value:1]" {
		t.Errorf("string fallback failed: text=%q err=%v", text, err)
	}
}

func TestValidation_RejectsUnsafeFragmentsAndTags(t *testing.T) {
	for _, fragment := range []string{"", "DROP", "value UNION SELECT other", "SUM(value) -- comment", "SUM(value); DELETE FROM ex_models"} {
		if err := ValidateRawQuery(fragment); !errors.Is(err, ErrInvalidColumnName) {
			t.Errorf("unsafe fragment accepted: %q err=%v", fragment, err)
		}
	}
	for _, column := range []string{"", "drop", "name union id", "name delete", "name;delete"} {
		if err := ValidateColumnName(column); !errors.Is(err, ErrInvalidColumnName) {
			t.Errorf("unsafe column accepted: %q err=%v", column, err)
		}
	}
	for _, tag := range []string{"", "1column", "a,b", "name.value"} {
		if err := validateBareColumnTag(tag); err == nil {
			t.Errorf("invalid bare tag accepted: %q", tag)
		}
	}
}

func TestDirtyAPI_NilAndUntrackedEntities(t *testing.T) {
	m := New[ExModel]()
	var entity *ExModel
	ClearOriginals(entity)
	trackOriginals(entity, m.modelInfo)
	if GetOriginal(entity, "name") != nil || GetOriginals(entity) != nil || IsTracked(entity) || m.IsEntityTracked(entity) || m.IsDirtyField(entity, "name") || m.HasDirtyFields(entity) || m.GetDirtyFields(entity) != nil {
		t.Error("nil entity must be handled without a baseline or dirty fields")
	}
	var scope *TrackingScope
	if scope.Len() != 0 {
		t.Error("nil scope must report zero entities")
	}
	scope.track(1, nil)
	entity = &ExModel{ID: 1, Name: "A"}
	if !m.HasDirtyFields(entity) {
		t.Error("untracked non-nil entity should be considered dirty")
	}
	trackOriginals(entity, m.modelInfo)
	defer ClearOriginals(entity)
	if !m.IsEntityTracked(entity) || m.GetOriginalValue(entity, "name") != "A" || m.GetOriginalValue(entity, "missing") != nil {
		t.Error("public tracking inspection returned wrong results")
	}
}

type alternatePrimary struct {
	Code string
	Name string
}

func (alternatePrimary) PrimaryKey() string { return "code" }

func TestModelParsing_PointersAndCustomPrimaryKey(t *testing.T) {
	if got := ParseModelType(reflect.TypeFor[*ExModel]()); got != ParseModel[ExModel]() {
		t.Error("pointer and value parsing should share metadata")
	}
	if info := ParseModel[alternatePrimary](); info.PrimaryKey != "code" || !info.Columns["code"].IsPrimary {
		t.Errorf("custom primary key was not recognized: %+v", info)
	}
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Error("non-struct models should be rejected")
		}
	}()
	ParseModel[int]()
}

func TestErrorFormatting_ValidationRelationAndEmptyArgs(t *testing.T) {
	err := &ValidationError{Field: "age", Value: -1, Message: "must be positive"}
	for _, part := range []string{"age", "-1", "must be positive"} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("validation context lost: %s", err.Error())
		}
	}
	if WrapRelationError("Children", "Parent", nil) != nil {
		t.Error("nil relation error must remain nil")
	}
	if got := formatArgs([]any{nil, true, 2.5}); got != "[<nil>, true, 2.5]" {
		t.Errorf("unexpected generic argument formatting: %s", got)
	}
	if isDollarQuoteStart("plain", 0) || isDollarQuoteStart("$", 0) {
		t.Error("non-tag fragments should not open a dollar quote")
	}
}
