package zorm

import (
	"math"
	"reflect"
	"testing"
)

func TestBuildInClause_PostgresArrayTypes(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected any
	}{
		{"int", []any{int(-1), int(2)}, []int64{-1, 2}},
		{"int32", []any{int32(-1), int32(2)}, []int64{-1, 2}},
		{"int64", []any{int64(math.MinInt64), int64(math.MaxInt64)}, []int64{math.MinInt64, math.MaxInt64}},
		{"uint", []any{uint(0), uint(2)}, []int64{0, 2}},
		{"uint32", []any{uint32(0), uint32(math.MaxUint32)}, []int64{0, math.MaxUint32}},
		{"uint64", []any{uint64(0), uint64(math.MaxInt64)}, []int64{0, math.MaxInt64}},
		{"string", []any{"", "Alice"}, []string{"", "Alice"}},
		{"float32", []any{float32(-1.5), float32(2.5)}, []float64{-1.5, 2.5}},
		{"float64", []any{float64(-1.5), float64(2.5)}, []float64{-1.5, 2.5}},
		{"bool", []any{true, false}, []bool{true, false}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, negate := range []bool{false, true} {
				fragment, args, err := buildInClauseWithOperator("value", tt.values, DialectPostgres, negate)
				if err != nil {
					t.Fatalf("buildInClauseWithOperator failed: %v", err)
				}
				expectedSQL := "value = ANY(?)"
				if negate {
					expectedSQL = "value <> ALL(?)"
				}
				if fragment != expectedSQL {
					t.Errorf("expected SQL %q, got %q", expectedSQL, fragment)
				}
				if len(args) != 1 {
					t.Fatalf("expected one array parameter, got %v", args)
				}
				if !reflect.DeepEqual(args[0], tt.expected) {
					t.Errorf("expected array %#v, got %#v", tt.expected, args[0])
				}
			}
		})
	}
}

func TestBuildInClause_PostgresMixedTypes(t *testing.T) {
	tests := []struct {
		name   string
		values []any
	}{
		{"int and string", []any{int(1), "2"}},
		{"int32 and int64", []any{int32(1), int64(2)}},
		{"int64 and int", []any{int64(1), int(2)}},
		{"uint and int", []any{uint(1), int(2)}},
		{"uint32 and uint64", []any{uint32(1), uint64(2)}},
		{"uint64 and uint32", []any{uint64(1), uint32(2)}},
		{"string and nil", []any{"Alice", nil}},
		{"float32 and float64", []any{float32(1.5), float64(2.5)}},
		{"float64 and float32", []any{float64(1.5), float32(2.5)}},
		{"bool and int", []any{true, int(1)}},
		{"nil first", []any{nil, "Alice"}},
		{"unsupported type", []any{int16(1), int16(2)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, negate := range []bool{false, true} {
				fragment, args, err := buildInClauseWithOperator("value", tt.values, DialectPostgres, negate)
				if err != nil {
					t.Fatalf("buildInClauseWithOperator failed: %v", err)
				}
				expectedSQL := "value IN (?,?)"
				if negate {
					expectedSQL = "value NOT IN (?,?)"
				}
				if fragment != expectedSQL {
					t.Errorf("expected SQL %q, got %q", expectedSQL, fragment)
				}
				if !reflect.DeepEqual(args, tt.values) {
					t.Errorf("expected original values %#v, got %#v", tt.values, args)
				}
			}
		})
	}
}
