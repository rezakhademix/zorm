package zorm

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
)

// ScalarQuery provides a query builder for single-column scalar results.
// T can be any type that sql.Rows.Scan supports: string, int, int64,
// float64, bool, time.Time, []byte, sql.Null* types, or any sql.Scanner.
//
// Example:
//
//	names, err := zorm.Query[string]().
//	    Table("roles").
//	    Select("name").
//	    Where("active", true).
//	    Get(ctx)
type ScalarQuery[T any] struct {
	db        *sql.DB
	tx        *sql.Tx
	tableName string
	column    string
	wheres    []string
	args      []any
	orderBys  []string
	groupBys  []string
	havings   []string
	distinct  bool
	limit     int
	offset    int
}

// Query creates a new scalar query builder for type T.
func Query[T any]() *ScalarQuery[T] {
	return &ScalarQuery[T]{
		db:     GetGlobalDB(),
		wheres: make([]string, 0, 4),
		args:   make([]any, 0, 4),
	}
}

// Table sets the table name for the query.
// Table names are validated to prevent SQL injection.
func (q *ScalarQuery[T]) Table(name string) *ScalarQuery[T] {
	if err := ValidateColumnName(name); err != nil {
		return q
	}
	q.tableName = name
	return q
}

// Select sets the column to select (single column only).
func (q *ScalarQuery[T]) Select(column string) *ScalarQuery[T] {
	if err := ValidateColumnName(column); err != nil {
		return q
	}
	q.column = column
	return q
}

// Where adds a WHERE condition.
// Supports multiple forms:
//
//	Where("column", value) -> column = ?
//	Where("column >", value) -> column > ?
//	Where("column", ">", value) -> column > ?
//	Where(map[string]any{"name": "John"}) -> name = ?
func (q *ScalarQuery[T]) Where(query any, args ...any) *ScalarQuery[T] {
	return q.addWhere("AND", query, args...)
}

// OrWhere adds an OR WHERE condition.
func (q *ScalarQuery[T]) OrWhere(query any, args ...any) *ScalarQuery[T] {
	return q.addWhere("OR", query, args...)
}

func (q *ScalarQuery[T]) addWhere(typ string, query any, args ...any) *ScalarQuery[T] {
	// Handle Map
	if conditionMap, ok := query.(map[string]any); ok {
		for k, v := range conditionMap {
			if err := ValidateColumnName(k); err != nil {
				continue
			}
			q.wheres = append(q.wheres, typ+" "+k+" = ?")
			q.args = append(q.args, v)
		}
		return q
	}

	// Handle String
	queryStr, ok := query.(string)
	if !ok {
		return q
	}

	switch len(args) {
	case 0:
		q.wheres = append(q.wheres, typ+" "+queryStr)
	case 1:
		// column, value -> column = value
		if err := ValidateColumnName(queryStr); err != nil {
			return q
		}
		sb := GetStringBuilder()
		sb.WriteString(queryStr)
		sb.WriteString(" = ?")
		q.wheres = append(q.wheres, typ+" "+sb.String())
		PutStringBuilder(sb)
		q.args = append(q.args, args[0])
	case 2:
		// column, operator, value -> column operator value
		if err := ValidateColumnName(queryStr); err != nil {
			return q
		}
		sb := GetStringBuilder()
		sb.WriteString(queryStr)
		sb.WriteByte(' ')
		sb.WriteString(args[0].(string))
		sb.WriteString(" ?")
		q.wheres = append(q.wheres, typ+" "+sb.String())
		PutStringBuilder(sb)
		q.args = append(q.args, args[1])
	default:
		// Assume raw query with placeholders
		q.wheres = append(q.wheres, typ+" "+queryStr)
		q.args = append(q.args, args...)
	}

	return q
}

// WhereIn adds a WHERE column IN (...) condition.
func (q *ScalarQuery[T]) WhereIn(column string, values []any) *ScalarQuery[T] {
	if err := ValidateColumnName(column); err != nil {
		return q
	}
	if len(values) == 0 {
		q.wheres = append(q.wheres, "AND 1=0")
		return q
	}

	sb := GetStringBuilder()
	sb.WriteString(column)
	sb.WriteString(" IN (")
	for i := range values {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('?')
	}
	sb.WriteByte(')')
	q.wheres = append(q.wheres, "AND "+sb.String())
	PutStringBuilder(sb)
	q.args = append(q.args, values...)
	return q
}

// WhereNull adds a WHERE column IS NULL condition.
func (q *ScalarQuery[T]) WhereNull(column string) *ScalarQuery[T] {
	if err := ValidateColumnName(column); err != nil {
		return q
	}
	q.wheres = append(q.wheres, "AND "+column+" IS NULL")
	return q
}

// WhereNotNull adds a WHERE column IS NOT NULL condition.
func (q *ScalarQuery[T]) WhereNotNull(column string) *ScalarQuery[T] {
	if err := ValidateColumnName(column); err != nil {
		return q
	}
	q.wheres = append(q.wheres, "AND "+column+" IS NOT NULL")
	return q
}

// OrderBy adds an ORDER BY clause.
func (q *ScalarQuery[T]) OrderBy(column, direction string) *ScalarQuery[T] {
	if err := ValidateColumnName(column); err != nil {
		return q
	}
	dir := strings.ToUpper(strings.TrimSpace(direction))
	if dir != "ASC" && dir != "DESC" {
		dir = "DESC"
	}

	sb := GetStringBuilder()
	sb.WriteString(column)
	sb.WriteByte(' ')
	sb.WriteString(dir)
	q.orderBys = append(q.orderBys, sb.String())
	PutStringBuilder(sb)
	return q
}

// Limit sets the LIMIT clause.
func (q *ScalarQuery[T]) Limit(n int) *ScalarQuery[T] {
	q.limit = n
	return q
}

// Offset sets the OFFSET clause.
func (q *ScalarQuery[T]) Offset(n int) *ScalarQuery[T] {
	q.offset = n
	return q
}

// Distinct adds DISTINCT to the query.
func (q *ScalarQuery[T]) Distinct() *ScalarQuery[T] {
	q.distinct = true
	return q
}

// GroupBy adds GROUP BY columns.
func (q *ScalarQuery[T]) GroupBy(columns ...string) *ScalarQuery[T] {
	for _, col := range columns {
		if err := ValidateColumnName(col); err != nil {
			continue
		}
		q.groupBys = append(q.groupBys, col)
	}
	return q
}

// Having adds a HAVING clause.
func (q *ScalarQuery[T]) Having(query string, args ...any) *ScalarQuery[T] {
	if err := ValidateRawQuery(query); err != nil {
		return q
	}

	if len(args) > 0 && !strings.Contains(query, "?") {
		query = strings.TrimSpace(query) + " ?"
	}
	q.havings = append(q.havings, query)
	q.args = append(q.args, args...)
	return q
}

// SetDB sets a specific database connection.
func (q *ScalarQuery[T]) SetDB(db *sql.DB) *ScalarQuery[T] {
	q.db = db
	return q
}

// WithTx uses a transaction for the query.
func (q *ScalarQuery[T]) WithTx(tx *Tx) *ScalarQuery[T] {
	q.tx = tx.Tx
	return q
}

// Get executes the query and returns all matching values.
func (q *ScalarQuery[T]) Get(ctx context.Context) ([]T, error) {
	query := q.buildQuery()

	rows, err := q.queryer().QueryContext(ctx, rebind(query), q.args...)
	if err != nil {
		return nil, WrapQueryError("SELECT", query, q.args, err)
	}
	defer rows.Close()

	// Pre-allocate results slice based on limit or default capacity
	initialCap := q.limit
	if initialCap <= 0 {
		initialCap = 64
	}
	results := make([]T, 0, initialCap)

	for rows.Next() {
		var val T
		if err := rows.Scan(&val); err != nil {
			return nil, WrapQueryError("SCAN", query, q.args, err)
		}
		results = append(results, val)
	}

	if err := rows.Err(); err != nil {
		return nil, WrapQueryError("SCAN", query, q.args, err)
	}

	return results, nil
}

// First returns the first matching value.
// Returns ErrRecordNotFound if no rows match.
// Uses Clone() to avoid mutating the original query's limit.
func (q *ScalarQuery[T]) First(ctx context.Context) (T, error) {
	clone := q.Clone()
	clone.limit = 1
	results, err := clone.Get(ctx)
	if err != nil {
		var zero T
		return zero, err
	}
	if len(results) == 0 {
		var zero T
		return zero, ErrRecordNotFound
	}
	return results[0], nil
}

// Count returns the count of matching rows.
// This ignores the Select column and uses COUNT(*).
func (q *ScalarQuery[T]) Count(ctx context.Context) (int64, error) {
	sb := GetStringBuilder()
	defer PutStringBuilder(sb)

	sb.WriteString("SELECT COUNT(*) FROM ")
	sb.WriteString(q.tableName)

	q.buildWhereClause(sb)

	query := strings.Clone(sb.String())

	var count int64
	err := q.queryer().QueryRowContext(ctx, rebind(query), q.args...).Scan(&count)
	if err != nil {
		return 0, WrapQueryError("COUNT", query, q.args, err)
	}
	return count, nil
}

// buildQuery constructs the SELECT query.
func (q *ScalarQuery[T]) buildQuery() string {
	sb := GetStringBuilder()
	defer PutStringBuilder(sb)

	sb.WriteString("SELECT ")
	if q.distinct {
		sb.WriteString("DISTINCT ")
	}

	if q.column != "" {
		sb.WriteString(q.column)
	} else {
		sb.WriteByte('*')
	}

	sb.WriteString(" FROM ")
	sb.WriteString(q.tableName)

	q.buildWhereClause(sb)

	if len(q.groupBys) > 0 {
		sb.WriteString(" GROUP BY ")
		sb.WriteString(strings.Join(q.groupBys, ", "))
	}

	if len(q.havings) > 0 {
		sb.WriteString(" HAVING ")
		sb.WriteString(strings.Join(q.havings, " AND "))
	}

	if len(q.orderBys) > 0 {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(strings.Join(q.orderBys, ", "))
	}

	if q.limit > 0 {
		sb.WriteString(" LIMIT ")
		sb.WriteString(strconv.Itoa(q.limit))
	}

	if q.offset > 0 {
		sb.WriteString(" OFFSET ")
		sb.WriteString(strconv.Itoa(q.offset))
	}

	// Clone the string to ensure it's independent of the builder's buffer.
	// The defer will return the builder to the pool, so we need a safe copy.
	return strings.Clone(sb.String())
}

// buildWhereClause appends WHERE conditions to the query builder.
func (q *ScalarQuery[T]) buildWhereClause(sb *strings.Builder) {
	if len(q.wheres) > 0 {
		sb.WriteString(" WHERE 1=1 ")
		for _, w := range q.wheres {
			sb.WriteByte(' ')
			sb.WriteString(w)
		}
	}
}

// Clone creates a deep copy of the query.
// This is useful for creating variations of a query without mutating the original.
func (q *ScalarQuery[T]) Clone() *ScalarQuery[T] {
	clone := &ScalarQuery[T]{
		db:        q.db,
		tx:        q.tx,
		tableName: q.tableName,
		column:    q.column,
		distinct:  q.distinct,
		limit:     q.limit,
		offset:    q.offset,
	}

	// Copy slices
	if len(q.wheres) > 0 {
		clone.wheres = make([]string, len(q.wheres))
		copy(clone.wheres, q.wheres)
	}
	if len(q.args) > 0 {
		clone.args = make([]any, len(q.args))
		copy(clone.args, q.args)
	}
	if len(q.orderBys) > 0 {
		clone.orderBys = make([]string, len(q.orderBys))
		copy(clone.orderBys, q.orderBys)
	}
	if len(q.groupBys) > 0 {
		clone.groupBys = make([]string, len(q.groupBys))
		copy(clone.groupBys, q.groupBys)
	}
	if len(q.havings) > 0 {
		clone.havings = make([]string, len(q.havings))
		copy(clone.havings, q.havings)
	}

	return clone
}

// queryer returns the database connection to use.
func (q *ScalarQuery[T]) queryer() interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
} {
	if q.tx != nil {
		return q.tx
	}

	// If resolver is configured, use it for routing
	if resolver := GetGlobalResolver(); resolver != nil {
		return resolver.Replica()
	}

	if q.db != nil {
		return q.db
	}
	return GetGlobalDB()
}

// Print returns the SQL query and arguments that would be executed without running it.
// This is useful for debugging and logging the generated SQL.
func (q *ScalarQuery[T]) Print() (string, []any) {
	return rebind(q.buildQuery()), q.args
}
