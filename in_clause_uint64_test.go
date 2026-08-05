package zorm

import (
	"math"
	"strings"
	"testing"
)

// toTypedArraySlice cast every uint64 straight to int64 so PostgreSQL could
// take it as a BIGINT array. Values above MaxInt64 wrapped to negative numbers
// and the query quietly matched the wrong rows — the worst failure mode there
// is, since nothing errors.

func TestBuildInClause_LargeUint64DoesNotWrap(t *testing.T) {
	const big = uint64(math.MaxInt64) + 1

	frag, args, err := buildInClause("id", []any{big}, DialectPostgres)
	if err != nil {
		t.Fatalf("buildInClause failed: %v", err)
	}

	if strings.Contains(frag, "ANY") {
		t.Errorf("a value that cannot be an int64 must not take the ANY-array path, got %q", frag)
	}
	if len(args) != 1 {
		t.Fatalf("expected 1 arg, got %v", args)
	}
	if got, ok := args[0].(uint64); !ok || got != big {
		t.Errorf("expected the original uint64 %d to survive, got %#v", big, args[0])
	}
}

// TestBuildNotInClause_LargeUint64DoesNotWrap covers the NOT IN complement,
// which shares the same conversion.
func TestBuildNotInClause_LargeUint64DoesNotWrap(t *testing.T) {
	const big = math.MaxUint64

	frag, args, err := buildNotInClause("id", []any{uint64(big)}, DialectPostgres)
	if err != nil {
		t.Fatalf("buildNotInClause failed: %v", err)
	}

	if strings.Contains(frag, "ALL") {
		t.Errorf("a value that cannot be an int64 must not take the ALL-array path, got %q", frag)
	}
	if got, ok := args[0].(uint64); !ok || got != uint64(big) {
		t.Errorf("expected the original uint64 to survive, got %#v", args[0])
	}
}

// TestBuildInClause_MixedUint64UsesPlaceholdersWhenAnyOverflows verifies one
// out-of-range value disqualifies the whole batch rather than corrupting it.
func TestBuildInClause_MixedUint64UsesPlaceholdersWhenAnyOverflows(t *testing.T) {
	vals := []any{uint64(1), uint64(math.MaxInt64) + 5, uint64(3)}

	frag, args, err := buildInClause("id", vals, DialectPostgres)
	if err != nil {
		t.Fatalf("buildInClause failed: %v", err)
	}
	if strings.Contains(frag, "ANY") {
		t.Errorf("expected the placeholder form, got %q", frag)
	}
	if len(args) != 3 {
		t.Fatalf("expected 3 args, got %v", args)
	}
	for i, v := range args {
		if _, ok := v.(uint64); !ok {
			t.Errorf("arg %d lost its type: %#v", i, v)
		}
	}
}

// TestBuildInClause_InRangeUint64StillUsesAnyFastPath is the overcorrection
// guard: ordinary values must keep the ANY-array fast path that sidesteps the
// 65535-parameter cap.
func TestBuildInClause_InRangeUint64StillUsesAnyFastPath(t *testing.T) {
	frag, args, err := buildInClause("id", []any{uint64(1), uint64(2)}, DialectPostgres)
	if err != nil {
		t.Fatalf("buildInClause failed: %v", err)
	}
	if !strings.Contains(frag, "ANY") {
		t.Errorf("expected the ANY-array fast path, got %q", frag)
	}
	if len(args) != 1 {
		t.Fatalf("expected the args to collapse to one array, got %v", args)
	}
	if _, ok := args[0].([]int64); !ok {
		t.Errorf("expected []int64 for pgx, got %T", args[0])
	}
}

// TestBuildInClause_LargeUintDoesNotWrap covers the `uint` case, which is
// 64 bits wide on every platform this library targets and had the same
// unchecked cast.
func TestBuildInClause_LargeUintDoesNotWrap(t *testing.T) {
	big := uint(math.MaxInt64) + 1

	frag, args, err := buildInClause("id", []any{big}, DialectPostgres)
	if err != nil {
		t.Fatalf("buildInClause failed: %v", err)
	}
	if strings.Contains(frag, "ANY") {
		t.Errorf("a value that cannot be an int64 must not take the ANY-array path, got %q", frag)
	}
	if got, ok := args[0].(uint); !ok || got != big {
		t.Errorf("expected the original uint %d to survive, got %#v", big, args[0])
	}
}
