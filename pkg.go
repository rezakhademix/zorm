package zorm

import (
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
)

// Dialect identifies the SQL dialect used to build queries. ZORM targets
// PostgreSQL in production but ships SQLite-backed tests, so a small number
// of code paths (notably large IN-list rewriting) branch on dialect.
type Dialect uint8

const (
	// DialectAuto defers detection until a *sql.DB is available; falls back
	// to DialectPostgres when no DB is set (e.g. for Print() of an unbound
	// query).
	DialectAuto Dialect = iota
	// DialectPostgres uses PostgreSQL-specific syntax such as `= ANY($1)`.
	DialectPostgres
	// DialectSQLite uses portable `IN (?, ?, ...)` syntax.
	DialectSQLite
)

// maxInArgs is PostgreSQL's hard per-statement bind-parameter limit (uint16).
// IN lists exceeding this on the SQLite fallback path return an error
// instead of generating an invalid query.
const maxInArgs = 65535

// globalDialect is the package-wide dialect override. Zero (DialectAuto)
// means "detect from the configured *sql.DB".
var globalDialect atomic.Uint32

// SetDialect overrides the package-wide dialect. Pass DialectAuto to restore
// driver-based detection.
func SetDialect(d Dialect) { globalDialect.Store(uint32(d)) }

// GetDialect returns the current package-wide dialect override.
func GetDialect() Dialect { return Dialect(globalDialect.Load()) }

// detectDialect resolves a concrete dialect for a model. The global override
// takes priority; otherwise the *sql.DB's driver type name is inspected.
// Unknown drivers default to DialectPostgres, matching ZORM's documented
// production target.
func detectDialect(db *sql.DB) Dialect {
	if d := GetDialect(); d != DialectAuto {
		return d
	}
	if db == nil {
		return DialectPostgres
	}
	name := fmt.Sprintf("%T", db.Driver())
	if strings.Contains(strings.ToLower(name), "sqlite") {
		return DialectSQLite
	}
	return DialectPostgres
}

// buildInClause emits a WHERE-IN style fragment appropriate for the dialect.
//
// On PostgreSQL with a slice whose elements share a supported scalar type
// (int / int32 / int64 / string / float64 / bool), it returns
// `col = ANY(?)` with a single typed-slice arg — sidestepping the 65535
// parameter limit via pgx's native array binding.
//
// On SQLite, or when the slice is mixed-type, it returns the classic
// `col IN (?, ?, ...)` form with spread args. SQLite/fallback inputs
// larger than maxInArgs return a non-nil error rather than silently
// generating SQL that the driver will reject.
//
// An empty args slice returns `1=0` (matches nothing).
func buildInClause(col string, args []any, dialect Dialect) (string, []any, error) {
	if len(args) == 0 {
		return "1=0", nil, nil
	}
	if dialect == DialectPostgres {
		if typed, ok := toTypedArraySlice(args); ok {
			return col + " = ANY(?)", []any{typed}, nil
		}
	}
	if len(args) > maxInArgs {
		return "", nil, fmt.Errorf("zorm: IN list of %d args exceeds PostgreSQL parameter limit %d; pass a typed []int64/[]string/[]float64/[]bool slice to enable the ANY-array fast path", len(args), maxInArgs)
	}
	var sb strings.Builder
	sb.Grow(len(col) + 5 + 2*len(args))
	sb.WriteString(col)
	sb.WriteString(" IN (")
	writePlaceholders(&sb, len(args))
	sb.WriteByte(')')
	out := make([]any, len(args))
	copy(out, args)
	return sb.String(), out, nil
}

// toTypedArraySlice converts a homogeneous []any into a concrete typed slice
// that pgx/v5 can encode as a PostgreSQL array parameter. Mixed-type or
// unsupported slices return (nil, false) so the caller can fall back.
func toTypedArraySlice(args []any) (any, bool) {
	switch args[0].(type) {
	case int:
		out := make([]int64, len(args))
		for i, v := range args {
			n, ok := v.(int)
			if !ok {
				return nil, false
			}
			out[i] = int64(n)
		}
		return out, true
	case int32:
		out := make([]int64, len(args))
		for i, v := range args {
			n, ok := v.(int32)
			if !ok {
				return nil, false
			}
			out[i] = int64(n)
		}
		return out, true
	case int64:
		out := make([]int64, len(args))
		for i, v := range args {
			n, ok := v.(int64)
			if !ok {
				return nil, false
			}
			out[i] = n
		}
		return out, true
	case string:
		out := make([]string, len(args))
		for i, v := range args {
			s, ok := v.(string)
			if !ok {
				return nil, false
			}
			out[i] = s
		}
		return out, true
	case float64:
		out := make([]float64, len(args))
		for i, v := range args {
			f, ok := v.(float64)
			if !ok {
				return nil, false
			}
			out[i] = f
		}
		return out, true
	case bool:
		out := make([]bool, len(args))
		for i, v := range args {
			b, ok := v.(bool)
			if !ok {
				return nil, false
			}
			out[i] = b
		}
		return out, true
	}
	return nil, false
}
