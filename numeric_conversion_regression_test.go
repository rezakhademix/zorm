package zorm

import (
	"context"
	"math"
	"reflect"
	"testing"
)

type narrowAmount int8
type wideAmount int64

func TestNumericConversion_RejectsOverflowWithoutMutation(t *testing.T) {
	cases := []struct {
		name  string
		typ   reflect.Type
		input any
	}{
		{"signed narrowing", reflect.TypeFor[int8](), int64(300)},
		{"unsigned narrowing", reflect.TypeFor[uint8](), uint64(256)},
		{"negative unsigned", reflect.TypeFor[uint64](), int64(-1)},
		{"unsigned to signed", reflect.TypeFor[int64](), uint64(math.MaxUint64)},
		{"float32 overflow", reflect.TypeFor[float32](), math.MaxFloat64},
		{"float signed boundary", reflect.TypeFor[int64](), math.Exp2(63)},
		{"float unsigned boundary", reflect.TypeFor[uint64](), math.Exp2(64)},
		{"negative float unsigned", reflect.TypeFor[uint64](), -1.5},
		{"NaN to integer", reflect.TypeFor[int64](), math.NaN()},
		{"infinity to integer", reflect.TypeFor[uint64](), math.Inf(1)},
		{"string overflow", reflect.TypeFor[int8](), "128"},
		{"byte string overflow", reflect.TypeFor[int8](), []byte("-129")},
		{"defined numeric types", reflect.TypeFor[narrowAmount](), wideAmount(300)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			field := reflect.New(tc.typ).Elem()
			field.Set(reflect.ValueOf(7).Convert(tc.typ))
			before := field.Interface()
			if err := setFieldValue(field, tc.input); err == nil {
				t.Errorf("accepted %v (%T) as %s", tc.input, tc.input, tc.typ)
			}
			if !reflect.DeepEqual(field.Interface(), before) {
				t.Errorf("failed conversion changed field from %v to %v", before, field.Interface())
			}
		})
	}
}

func TestNumericConversion_PreservesValidBoundaries(t *testing.T) {
	cases := []struct{ input, want any }{
		{int64(-128), int8(-128)},
		{int64(127), int8(127)},
		{uint64(255), uint8(255)},
		{uint64(math.MaxInt64), int64(math.MaxInt64)},
		{float64(math.MaxFloat32), float32(math.MaxFloat32)},
		{float64(10.9), int8(10)},
		{float64(10.5), uint8(10)},
		{"127", int8(127)},
		{[]byte("255"), uint8(255)},
		{wideAmount(127), narrowAmount(127)},
	}
	for _, tc := range cases {
		field := reflect.New(reflect.TypeOf(tc.want)).Elem()
		if err := setFieldValue(field, tc.input); err != nil {
			t.Errorf("valid conversion %v to %T failed: %v", tc.input, tc.want, err)
		} else if !reflect.DeepEqual(field.Interface(), tc.want) {
			t.Errorf("conversion %v: got %v, want %v", tc.input, field.Interface(), tc.want)
		}
	}
}

func TestNumericConversion_FailedPointerAssignmentDoesNotAllocate(t *testing.T) {
	var amount *int8
	field := reflect.ValueOf(&amount).Elem()
	if err := setFieldValue(field, int64(300)); err == nil {
		t.Error("expected overflow error")
	}
	if amount != nil {
		t.Errorf("failed conversion allocated a pointer containing %d", *amount)
	}
	if err := setFieldValue(field, int64(127)); err != nil || amount == nil || *amount != 127 {
		t.Fatalf("valid pointer assignment failed: %v, %v", amount, err)
	}
}

type numericItem struct {
	ID     int
	Amount int8
}

func (numericItem) TableName() string { return "numeric_items" }

func TestNumericConversion_UpdateOrCreateRejectsCorruptingInput(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	_, err := db.Exec("CREATE TABLE numeric_items (id INTEGER PRIMARY KEY, amount INTEGER); INSERT INTO numeric_items VALUES (1, 7)")
	if err != nil {
		t.Fatal(err)
	}
	m := New[numericItem]().SetDB(db)
	for _, id := range []int{1, 2} {
		_, err := m.UpdateOrCreate(context.Background(), map[string]any{"id": id}, map[string]any{"amount": int64(300)})
		if err == nil {
			t.Errorf("id %d: overflowing input reached the database", id)
		}
	}
	var count, amount int
	if err := db.QueryRow("SELECT COUNT(*), SUM(amount) FROM numeric_items").Scan(&count, &amount); err != nil {
		t.Fatal(err)
	}
	if count != 1 || amount != 7 {
		t.Errorf("overflow changed stored data: count=%d, amount=%d", count, amount)
	}
}
