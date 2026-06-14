package zorm

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
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

// driverDialectRegistry maps a driver's Go type name (as produced by
// fmt.Sprintf("%T", driver)) to the dialect it speaks. Populated by
// RegisterDriverDialect — callers can register custom or instrumented
// driver wrappers whose names don't contain a recognizable substring.
var (
	driverDialectMu       sync.RWMutex
	driverDialectRegistry = map[string]Dialect{
		// Known drivers — pre-registered so basic setups work without
		// any explicit registration call.
		"*sqlite3.SQLiteDriver": DialectSQLite,
		"*stdlib.Driver":        DialectPostgres, // pgx/v5/stdlib
		"*pq.Driver":            DialectPostgres, // lib/pq
	}
)

// RegisterDriverDialect maps a driver's Go type name (e.g. "*otelsql.Driver")
// to a dialect, overriding ZORM's built-in detection. Use this when running
// behind an instrumentation wrapper whose type name does not encode the
// underlying database.
//
// Pass the exact string produced by fmt.Sprintf("%T", db.Driver()).
// Calling with DialectAuto removes any prior registration.
func RegisterDriverDialect(driverTypeName string, d Dialect) {
	driverDialectMu.Lock()
	defer driverDialectMu.Unlock()
	if d == DialectAuto {
		delete(driverDialectRegistry, driverTypeName)
		return
	}
	driverDialectRegistry[driverTypeName] = d
}

// detectDialect resolves a concrete dialect for a model. Resolution order:
//  1. package-wide SetDialect override
//  2. RegisterDriverDialect entry for the driver's type name (exact match)
//  3. substring sniff on the driver's type name (legacy fallback)
//  4. DialectPostgres
//
// Unknown drivers default to DialectPostgres, matching ZORM's documented
// production target. The substring sniff is preserved for backwards
// compatibility but is intentionally only the third choice — explicit
// registration is preferred and avoids false positives from wrapper
// libraries whose type names happen to contain "sqlite" or similar.
func detectDialect(db *sql.DB) Dialect {
	if d := GetDialect(); d != DialectAuto {
		return d
	}
	if db == nil {
		return DialectPostgres
	}
	name := fmt.Sprintf("%T", db.Driver())
	driverDialectMu.RLock()
	if d, ok := driverDialectRegistry[name]; ok {
		driverDialectMu.RUnlock()
		return d
	}
	driverDialectMu.RUnlock()
	if strings.Contains(strings.ToLower(name), "sqlite") {
		return DialectSQLite
	}
	return DialectPostgres
}

// buildInClause emits a WHERE-IN style fragment appropriate for the dialect.
//
// On PostgreSQL with a slice whose elements share a supported scalar type
// (int / int32 / int64 / uint / uint32 / uint64 / string / float32 / float64
// / bool), it returns `col = ANY(?)` with a single typed-slice arg —
// sidestepping the 65535 parameter limit via pgx's native array binding.
//
// On SQLite, or when the slice is mixed-type or of an unsupported element
// type, it returns the classic `col IN (?, ?, ...)` form with spread args.
// SQLite/fallback inputs larger than maxInArgs return a non-nil error
// rather than silently generating SQL that the driver will reject.
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
		return "", nil, fmt.Errorf("zorm: IN list of %d args exceeds PostgreSQL parameter limit %d; pass a typed []int64/[]uint64/[]string/[]float64/[]bool slice (homogeneous) to enable the ANY-array fast path", len(args), maxInArgs)
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
	case uint:
		out := make([]int64, len(args))
		for i, v := range args {
			n, ok := v.(uint)
			if !ok {
				return nil, false
			}
			out[i] = int64(n)
		}
		return out, true
	case uint32:
		out := make([]int64, len(args))
		for i, v := range args {
			n, ok := v.(uint32)
			if !ok {
				return nil, false
			}
			out[i] = int64(n)
		}
		return out, true
	case uint64:
		out := make([]int64, len(args))
		for i, v := range args {
			n, ok := v.(uint64)
			if !ok {
				return nil, false
			}
			// Cast to int64; values above MaxInt64 are not representable as
			// PostgreSQL BIGINT and would already be broken elsewhere.
			out[i] = int64(n)
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
	case float32:
		out := make([]float64, len(args))
		for i, v := range args {
			f, ok := v.(float32)
			if !ok {
				return nil, false
			}
			out[i] = float64(f)
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
