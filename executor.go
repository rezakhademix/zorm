package zorm

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// queryer returns the appropriate query executor based on transaction state and resolver configuration.
// If a transaction is active (m.tx != nil), it returns the transaction executor.
// If GlobalResolver is configured, it routes based on forcePrimary/forceReplica flags.
// Otherwise, it returns the database connection executor.
// This allows the ORM to seamlessly work with both transactional and non-transactional contexts,
// as well as primary/replica setups.
func (m *Model[T]) queryer() interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
} {
	// Transactions always use their own connection
	if m.tx != nil {
		return m.tx
	}

	// If resolver is configured, use it for routing
	if GlobalResolver != nil {
		return m.resolveDB()
	}

	// Fallback to model or global DB
	if m.db != nil {
		return m.db
	}
	return GlobalDB
}

// resolveDB determines which database connection to use based on resolver configuration.
func (m *Model[T]) resolveDB() *sql.DB {
	// Manual override: force primary
	if m.forcePrimary {
		return GlobalResolver.Primary()
	}

	// Manual override: force specific replica
	if m.forceReplica >= 0 {
		db := GlobalResolver.ReplicaAt(m.forceReplica)
		if db != nil {
			return db
		}
		// Fallback to load-balanced replica if index is invalid
	}

	// Auto-select replica (load balanced)
	// For read operations, this will be called by executor
	return GlobalResolver.Replica()
}

// queryerForWrite returns the primary database for write operations.
// This should be used by Create, Update, Delete, and other write methods.
func (m *Model[T]) queryerForWrite() interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
} {
	// Transactions always use their own connection
	if m.tx != nil {
		return m.tx
	}

	// If resolver is configured, always use primary for writes
	if GlobalResolver != nil {
		return GlobalResolver.Primary()
	}

	// Fallback to model or global DB
	if m.db != nil {
		return m.db
	}
	return GlobalDB
}

// prepareStmt returns a prepared statement for the given query.
// If statement caching is enabled (m.stmtCache != nil), it attempts to:
// 1. Retrieve the statement from cache
// 2. If not found, prepare the statement and cache it
// If caching is not enabled, it prepares the statement directly without caching.
//
// Note: The caller is responsible for executing the statement, but should NOT close it
// when caching is enabled, as cached statements are reused and managed by the cache.
func (m *Model[T]) prepareStmt(ctx context.Context, query string) (*sql.Stmt, error) {
	// If caching is not enabled, prepare directly
	if m.stmtCache == nil {
		q := m.queryer()
		// We need the underlying *sql.DB or *sql.Tx to prepare
		if db, ok := q.(*sql.DB); ok {
			return db.PrepareContext(ctx, query)
		}
		if tx, ok := q.(*sql.Tx); ok {
			return tx.PrepareContext(ctx, query)
		}
		// Fallback: should not happen, but handle it
		return nil, fmt.Errorf("unable to prepare statement: invalid queryer type")
	}

	// Try to get from cache
	if stmt := m.stmtCache.Get(query); stmt != nil {
		return stmt, nil
	}

	// Not in cache, prepare it
	q := m.queryer()
	var stmt *sql.Stmt
	var err error

	if db, ok := q.(*sql.DB); ok {
		stmt, err = db.PrepareContext(ctx, query)
	} else if tx, ok := q.(*sql.Tx); ok {
		stmt, err = tx.PrepareContext(ctx, query)
	} else {
		return nil, fmt.Errorf("unable to prepare statement: invalid queryer type")
	}

	if err != nil {
		return nil, err
	}

	// Store in cache
	m.stmtCache.Put(query, stmt)
	return stmt, nil
}

// prepareStmtForWrite returns a prepared statement for write operations.
// Similar to prepareStmt but uses queryerForWrite to ensure primary database is used.
func (m *Model[T]) prepareStmtForWrite(ctx context.Context, query string) (*sql.Stmt, error) {
	// If caching is not enabled, prepare directly
	if m.stmtCache == nil {
		q := m.queryerForWrite()
		if db, ok := q.(*sql.DB); ok {
			return db.PrepareContext(ctx, query)
		}
		if tx, ok := q.(*sql.Tx); ok {
			return tx.PrepareContext(ctx, query)
		}
		return nil, fmt.Errorf("unable to prepare statement: invalid queryer type")
	}

	// Try to get from cache
	if stmt := m.stmtCache.Get(query); stmt != nil {
		return stmt, nil
	}

	// Not in cache, prepare it
	q := m.queryerForWrite()
	var stmt *sql.Stmt
	var err error

	if db, ok := q.(*sql.DB); ok {
		stmt, err = db.PrepareContext(ctx, query)
	} else if tx, ok := q.(*sql.Tx); ok {
		stmt, err = tx.PrepareContext(ctx, query)
	} else {
		return nil, fmt.Errorf("unable to prepare statement: invalid queryer type")
	}

	if err != nil {
		return nil, err
	}

	// Store in cache
	m.stmtCache.Put(query, stmt)
	return stmt, nil
}

// Get executes the query and returns a slice of results.
func (m *Model[T]) Get(ctx context.Context) ([]*T, error) {
	query, args := m.buildSelectQuery()

	var rows *sql.Rows
	var err error

	// Use prepared statement if caching is enabled
	if m.stmtCache != nil {
		var stmt *sql.Stmt
		stmt, err = m.prepareStmt(ctx, query)
		if err != nil {
			return nil, WrapQueryError("PREPARE", query, args, err)
		}

		rows, err = stmt.QueryContext(ctx, args...)
	} else {
		rows, err = m.queryer().QueryContext(ctx, query, args...)
	}

	if err != nil {
		return nil, WrapQueryError("SELECT", query, args, err)
	}
	defer rows.Close()

	results, err := m.scanRows(rows)
	if err != nil {
		return nil, WrapQueryError("SCAN", query, args, err)
	}

	if err := m.loadRelations(ctx, results); err != nil {
		return nil, err
	}

	return results, nil
}

// First executes the query and returns the first result.
func (m *Model[T]) First(ctx context.Context) (*T, error) {
	// Enforce limit 1
	m.limit = 1
	results, err := m.Get(ctx)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, ErrRecordNotFound
	}
	return results[0], nil
}

// Find finds a record by ID.
func (m *Model[T]) Find(ctx context.Context, id any) (*T, error) {
	return m.Where(m.modelInfo.PrimaryKey+" = ?", id).First(ctx)
}

// FindOrFail finds a record by ID or returns an error.
// In Go, this is identical to Find, but added for API parity.
func (m *Model[T]) FindOrFail(ctx context.Context, id any) (*T, error) {
	return m.Find(ctx, id)
}

// Pluck retrieves a single column's values from the result set.
func (m *Model[T]) Pluck(ctx context.Context, column string) ([]any, error) {
	// We only select the specific column
	// But we need to be careful not to overwrite existing select if user manually selected?
	// Usually Pluck overrides select.
	m.columns = []string{column}

	query, args := m.buildSelectQuery()
	rows, err := m.queryer().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, WrapQueryError("SELECT", query, args, err)
	}
	defer rows.Close()

	var results []any
	for rows.Next() {
		var val any
		if err := rows.Scan(&val); err != nil {
			return nil, WrapQueryError("SCAN", query, args, err)
		}
		results = append(results, val)
	}

	if err := rows.Err(); err != nil {
		return nil, WrapQueryError("SCAN", query, args, err)
	}

	return results, nil
}

// Count returns the number of records matching the query.
func (m *Model[T]) Count(ctx context.Context) (int64, error) {
	// Backup limit/offset/order
	limit, offset := m.limit, m.offset
	orderBys := m.orderBys

	// Reset for count
	m.limit, m.offset = 0, 0
	m.orderBys = nil

	tableName := m.modelInfo.TableName
	var sb strings.Builder
	cteArgs := m.buildWithClause(&sb)

	sb.WriteString("SELECT COUNT(*) FROM ")
	sb.WriteString(tableName)

	m.buildWhereClause(&sb)

	query := sb.String()
	args := append(cteArgs, m.args...)

	// Restore state
	m.limit, m.offset = limit, offset
	m.orderBys = orderBys

	var count int64
	var err error

	// Use prepared statement if caching is enabled
	if m.stmtCache != nil {
		var stmt *sql.Stmt
		stmt, err = m.prepareStmt(ctx, query)
		if err != nil {
			return 0, WrapQueryError("PREPARE", query, args, err)
		}
		err = stmt.QueryRowContext(ctx, args...).Scan(&count)
	} else {
		err = m.queryer().QueryRowContext(ctx, query, args...).Scan(&count)
	}

	if err != nil {
		return 0, WrapQueryError("COUNT", query, args, err)
	}

	return count, nil
}

// Sum calculates the sum of a column.
// Returns 0 if no rows match or the sum is null.
func (m *Model[T]) Sum(ctx context.Context, column string) (float64, error) {
	// Backup limit/offset/order
	limit, offset := m.limit, m.offset
	orderBys := m.orderBys

	// Reset for sum
	m.limit, m.offset = 0, 0
	m.orderBys = nil

	tableName := m.modelInfo.TableName
	var sb strings.Builder
	cteArgs := m.buildWithClause(&sb)

	sb.WriteString("SELECT SUM(")
	sb.WriteString(column)
	sb.WriteString(") FROM ")
	sb.WriteString(tableName)

	m.buildWhereClause(&sb)

	query := sb.String()
	args := append(cteArgs, m.args...)

	// Restore state
	m.limit, m.offset = limit, offset
	m.orderBys = orderBys

	var result sql.NullFloat64
	var err error

	// Use prepared statement if caching is enabled
	if m.stmtCache != nil {
		var stmt *sql.Stmt
		stmt, err = m.prepareStmt(ctx, query)
		if err != nil {
			return 0, WrapQueryError("PREPARE", query, args, err)
		}
		err = stmt.QueryRowContext(ctx, args...).Scan(&result)
	} else {
		err = m.queryer().QueryRowContext(ctx, query, args...).Scan(&result)
	}

	if err != nil {
		return 0, WrapQueryError("SUM", query, args, err)
	}

	if result.Valid {
		return result.Float64, nil
	}
	return 0, nil
}

// Avg calculates the average of a column.
// Returns 0 if no rows match or the average is null.
func (m *Model[T]) Avg(ctx context.Context, column string) (float64, error) {
	// Backup limit/offset/order
	limit, offset := m.limit, m.offset
	orderBys := m.orderBys

	// Reset for avg
	m.limit, m.offset = 0, 0
	m.orderBys = nil

	tableName := m.modelInfo.TableName
	var sb strings.Builder
	cteArgs := m.buildWithClause(&sb)

	sb.WriteString("SELECT AVG(")
	sb.WriteString(column)
	sb.WriteString(") FROM ")
	sb.WriteString(tableName)

	m.buildWhereClause(&sb)

	query := sb.String()
	args := append(cteArgs, m.args...)

	// Restore state
	m.limit, m.offset = limit, offset
	m.orderBys = orderBys

	var result sql.NullFloat64
	var err error

	// Use prepared statement if caching is enabled
	if m.stmtCache != nil {
		var stmt *sql.Stmt
		stmt, err = m.prepareStmt(ctx, query)
		if err != nil {
			return 0, WrapQueryError("PREPARE", query, args, err)
		}
		err = stmt.QueryRowContext(ctx, args...).Scan(&result)
	} else {
		err = m.queryer().QueryRowContext(ctx, query, args...).Scan(&result)
	}

	if err != nil {
		return 0, WrapQueryError("AVG", query, args, err)
	}

	if result.Valid {
		return result.Float64, nil
	}
	return 0, nil
}

// CountOver returns count of records partitioned by the specified column.
// This uses window functions: COUNT(*) OVER (PARTITION BY column).
// Returns a map of column value -> count.
func (m *Model[T]) CountOver(ctx context.Context, column string) (map[any]int64, error) {
	// Build query: SELECT column, COUNT(*) OVER (PARTITION BY column) as count
	query := fmt.Sprintf("SELECT %s, COUNT(*) OVER (PARTITION BY %s) as count FROM %s",
		column, column, m.modelInfo.TableName)

	// Add WHERE clause
	var sb strings.Builder
	sb.WriteString(query)
	m.buildWhereClause(&sb)

	rows, err := m.queryer().QueryContext(ctx, sb.String(), m.args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[any]int64)
	for rows.Next() {
		var colVal any
		var count int64
		if err := rows.Scan(&colVal, &count); err != nil {
			return nil, err
		}
		result[colVal] = count
	}

	return result, rows.Err()
}

// buildSelectQuery constructs the SQL SELECT statement from the query builder state.
// It handles SELECT with DISTINCT, DISTINCT ON (PostgreSQL), columns, WHERE, GROUP BY,
// HAVING, ORDER BY, LIMIT, and OFFSET clauses.
// Returns the complete SQL query string and its corresponding arguments.
func (m *Model[T]) buildSelectQuery() (string, []any) {
	if m.rawQuery != "" {
		return m.rawQuery, m.rawArgs
	}

	sb := GetStringBuilder()
	defer PutStringBuilder(sb)

	cteArgs := m.buildWithClause(sb)

	sb.WriteString("SELECT ")

	if len(m.distinctOn) > 0 {
		// PostgreSQL DISTINCT ON syntax
		sb.WriteString("DISTINCT ON (")
		sb.WriteString(strings.Join(m.distinctOn, ", "))
		sb.WriteString(") ")
	} else if m.distinct {
		sb.WriteString("DISTINCT ")
	}

	if len(m.columns) > 0 {
		sb.WriteString(strings.Join(m.columns, ", "))
	} else {
		sb.WriteString("*")
	}

	sb.WriteString(" FROM ")
	sb.WriteString(m.modelInfo.TableName)

	m.buildWhereClause(sb)

	if len(m.groupBys) > 0 {
		sb.WriteString(" GROUP BY ")
		sb.WriteString(strings.Join(m.groupBys, ", "))
	}

	if len(m.havings) > 0 {
		sb.WriteString(" HAVING ")
		sb.WriteString(strings.Join(m.havings, " AND "))
	}

	if len(m.orderBys) > 0 {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(strings.Join(m.orderBys, ", "))
	}

	if m.lockMode != "" {
		sb.WriteString(" FOR ")
		sb.WriteString(m.lockMode)
	}

	if m.limit > 0 {
		sb.WriteString(" LIMIT ")
		sb.WriteString(strconv.Itoa(m.limit))
	}

	if m.offset > 0 {
		sb.WriteString(" OFFSET ")
		sb.WriteString(strconv.Itoa(m.offset))
	}

	// Pre-allocate args slice with correct capacity
	allArgs := make([]any, 0, len(cteArgs)+len(m.args))
	allArgs = append(allArgs, cteArgs...)
	allArgs = append(allArgs, m.args...)

	return sb.String(), allArgs
}

// buildWithClause constructs the WITH clause for CTEs.
func (m *Model[T]) buildWithClause(sb *strings.Builder) []any {
	if len(m.ctes) == 0 {
		return nil
	}

	sb.WriteString("WITH ")
	var args []any

	for i, cte := range m.ctes {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(cte.Name)
		sb.WriteString(" AS (")

		if q, ok := cte.Query.(string); ok {
			sb.WriteString(q)
		} else if subBuilder, ok := cte.Query.(interface {
			buildSelectQuery() (string, []any)
		}); ok {
			subQuery, subArgs := subBuilder.buildSelectQuery()
			sb.WriteString(subQuery)
			args = append(args, subArgs...)
		}

		sb.WriteString(")")
	}
	sb.WriteString(" ")
	return args
}

// buildWhereClause appends WHERE conditions to the query builder.
// It uses "WHERE 1=1" as a base to simplify appending AND/OR conditions.
func (m *Model[T]) buildWhereClause(sb *strings.Builder) {
	if len(m.wheres) > 0 {
		sb.WriteString(" WHERE 1=1 ") // Simplifies appending AND/OR
		for _, w := range m.wheres {
			sb.WriteString(" ")
			sb.WriteString(w)
		}
	}
}

// prepareScanDestinations creates scan destinations for sql.Rows.Scan based on column names.
// It maps database columns to struct fields using the model's column metadata.
// Columns not found in the struct are ignored to allow SELECT with extra columns.
// Returns a slice of pointers that can be passed to rows.Scan().
func (m *Model[T]) prepareScanDestinations(columns []string, val reflect.Value) []any {
	dest := make([]any, len(columns))

	for i, colName := range columns {
		if fieldInfo, ok := m.modelInfo.Columns[colName]; ok {
			// Get the field value and take its address for scanning
			// Both pointer and non-pointer fields use Addr() since sql.Scan needs **Type or *Type
			fieldVal := val.FieldByName(fieldInfo.Name)
			dest[i] = fieldVal.Addr().Interface()
		} else {
			// Column not in struct, ignore it
			var ignore any
			dest[i] = &ignore
		}
	}

	return dest
}

// scanRows scans sql.Rows into a slice of *T.
// It uses prepareScanDestinations to map columns to struct fields.
func (m *Model[T]) scanRows(rows *sql.Rows) ([]*T, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Pre-allocate results slice based on limit or default capacity
	initialCap := m.limit
	if initialCap <= 0 {
		initialCap = 64 // Default capacity for unbounded queries
	}
	results := make([]*T, 0, initialCap)

	for rows.Next() {
		// Create new instance of T
		entity := new(T)
		val := reflect.ValueOf(entity).Elem()

		// Prepare scan destinations using helper
		dest := m.prepareScanDestinations(columns, val)

		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}

		results = append(results, entity)
	}

	// Load Accessors
	m.loadAccessors(results)

	return results, rows.Err()
}

// Cursor returns a cursor for iterating over results one by one.
// Useful for large datasets to avoid loading everything into memory.
func (m *Model[T]) Cursor(ctx context.Context) (*Cursor[T], error) {
	query, args := m.buildSelectQuery()
	rows, err := m.queryer().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return &Cursor[T]{
		rows:  rows,
		model: m,
	}, nil
}

// Cursor provides a typed, forward-only iterator over database query results.
// It wraps sql.Rows and maps each row into the generic model type T.
type Cursor[T any] struct {
	rows    *sql.Rows
	model   *Model[T]
	columns []string // Cached column names to avoid repeated calls
}

// Next prepares the next result row for reading with the Scan method.
func (c *Cursor[T]) Next() bool {
	return c.rows.Next()
}

// Scan scans the current row into a new entity.
func (c *Cursor[T]) Scan() (*T, error) {
	// Cache columns on first call to avoid repeated lookups
	if c.columns == nil {
		var err error
		c.columns, err = c.rows.Columns()
		if err != nil {
			return nil, err
		}
	}

	entity := new(T)
	val := reflect.ValueOf(entity).Elem()

	// Use helper to prepare scan destinations
	dest := c.model.prepareScanDestinations(c.columns, val)

	if err := c.rows.Scan(dest...); err != nil {
		return nil, err
	}

	// Load Accessors
	c.model.loadAccessors([]*T{entity})

	// Load Relations if any are configured
	if len(c.model.relations) > 0 {
		// Used to use m.ctx, but now Cursor doesn't store context explicitly for Next/Scan.
		// However, Cursor was created with a context.
		// Wait, Cursor Scan method doesn't take context.
		// It should probably take context or use the one from Cursor creation?
		// Cursor struct has `rows *sql.Rows`, but `model *Model`.
		// If we want Scan to be context aware for lazy loading, we might need to change Cursor.Scan to take ctx.
		// For now, let's use the model's stored context as fallback or TODO.
		// Actually, let's look at Cursor definition.
		// The prompt says "make all methods context aware".
		// `Cursor.Scan()` -> `Cursor.Scan(ctx context.Context)`?
		// Let's do that in a separate step or here if possible.
		// But Cursor Scan mainly just reads from rows.
		// BUT `loadRelations` triggers queries. So it DOES need context.
		// I will check Cursor Scan signature change in next step. For now, let's fix the call to match.
		if err := c.model.loadRelations(c.model.ctx, []*T{entity}); err != nil {
			return nil, err
		}
	}

	return entity, nil
}

// Close closes the cursor.
func (c *Cursor[T]) Close() error {
	return c.rows.Close()
}

// FirstOrCreate finds the first record matching attributes or creates it with attributes+values.
// If found, returns the existing record. If not found, creates a new record with merged attributes+values.
func (m *Model[T]) FirstOrCreate(ctx context.Context, attributes map[string]any, values map[string]any) (*T, error) {
	// Validate inputs
	if attributes == nil {
		attributes = make(map[string]any)
	}
	if values == nil {
		values = make(map[string]any)
	}

	// Build query from attributes
	q := m
	for k, v := range attributes {
		q = q.Where(k+" = ?", v)
	}

	result, err := q.First(ctx)
	if err == nil && result != nil {
		return result, nil
	}

	// Not found, create
	// Merge attributes and values
	data := make(map[string]any)
	maps.Copy(data, attributes)
	maps.Copy(data, values)

	entity := new(T)
	if err := fillStruct(entity, data); err != nil {
		return nil, err
	}

	if err := m.Create(ctx, entity); err != nil {
		return nil, err
	}
	return entity, nil
}

// UpdateOrCreate finds a record matching attributes and updates it with values, or creates it.
// If found, updates the record with values. If not found, creates a new record with merged attributes+values.
func (m *Model[T]) UpdateOrCreate(ctx context.Context, attributes map[string]any, values map[string]any) (*T, error) {
	// Validate inputs
	if attributes == nil {
		attributes = make(map[string]any)
	}
	if values == nil {
		values = make(map[string]any)
	}

	// Build query from attributes
	q := m
	for k, v := range attributes {
		q = q.Where(k+" = ?", v)
	}

	result, err := q.First(ctx)
	if err == nil && result != nil {
		// Found, update
		if err := fillStruct(result, values); err != nil {
			return nil, err
		}
		// We need to update only the changed fields? Or all values?
		// Update() updates all fields of the struct currently.
		if err := m.Update(ctx, result); err != nil {
			return nil, err
		}
		return result, nil
	}

	// Not found, create
	data := make(map[string]any)
	maps.Copy(data, attributes)
	maps.Copy(data, values)

	entity := new(T)
	if err := fillStruct(entity, data); err != nil {
		return nil, err
	}

	if err := m.Create(ctx, entity); err != nil {
		return nil, err
	}
	return entity, nil
}

// scanRowsDynamic scans rows into a slice of pointers to structs defined by modelInfo.
// This is used for loading relations with different model types than T.
// It dynamically creates instances based on the provided ModelInfo.
func (m *Model[T]) scanRowsDynamic(rows *sql.Rows, modelInfo *ModelInfo) ([]any, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []any

	for rows.Next() {
		// Create new instance of the struct type
		val := reflect.New(modelInfo.Type) // *User
		elem := val.Elem()                 // User

		// Prepare destinations - need inline version since this uses different modelInfo
		dest := make([]any, len(columns))
		for i, colName := range columns {
			if fieldInfo, ok := modelInfo.Columns[colName]; ok {
				fieldVal := elem.FieldByName(fieldInfo.Name)
				dest[i] = fieldVal.Addr().Interface()
			} else {
				var ignore any
				dest[i] = &ignore
			}
		}

		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}

		results = append(results, val.Interface())
	}

	// Load Accessors

	return results, rows.Err()
}

// Create inserts a new record.
func (m *Model[T]) Create(ctx context.Context, entity *T) error {
	// Validate input
	if entity == nil {
		return ErrNilPointer
	}

	// 1. BeforeCreate Hook
	if hook, ok := any(entity).(interface{ BeforeCreate(context.Context) error }); ok {
		if err := hook.BeforeCreate(ctx); err != nil {
			return err
		}
	}

	// 2. Build Insert Query
	var columns []string
	var values []any
	var placeholders []string

	val := reflect.ValueOf(entity).Elem()

	for _, field := range m.modelInfo.Fields {
		// Skip auto-increment primary key if zero
		if field.IsPrimary && field.IsAuto {
			fVal := val.FieldByName(field.Name)
			if fVal.IsZero() {
				continue
			}
		}

		columns = append(columns, field.Column)
		values = append(values, val.FieldByName(field.Name).Interface())
		placeholders = append(placeholders, "?")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING %s",
		m.modelInfo.TableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		m.modelInfo.PrimaryKey,
	)

	// 3. Execute and scan ID directly into the primary key field
	pkField, ok := m.modelInfo.Columns[m.modelInfo.PrimaryKey]
	if !ok {
		return fmt.Errorf("primary key field %s not found in model", m.modelInfo.PrimaryKey)
	}

	fVal := val.FieldByName(pkField.Name)
	if !fVal.CanSet() {
		return fmt.Errorf("cannot set primary key field %s", pkField.Name)
	}

	var err error
	// Use prepared statement if caching is enabled
	if m.stmtCache != nil {
		var stmt *sql.Stmt
		stmt, err = m.prepareStmtForWrite(ctx, query)
		if err != nil {
			return WrapQueryError("PREPARE", query, values, err)
		}
		err = stmt.QueryRowContext(ctx, values...).Scan(fVal.Addr().Interface())
	} else {
		err = m.queryerForWrite().QueryRowContext(ctx, query, values...).Scan(fVal.Addr().Interface())
	}

	if err != nil {
		return WrapQueryError("INSERT", query, values, err)
	}

	// 4. AfterCreate Hook
	if hook, ok := any(entity).(interface{ AfterCreate(context.Context) error }); ok {
		if err := hook.AfterCreate(ctx); err != nil {
			return err
		}
	}

	return nil
}

// Update updates a single record based on its primary key.
// The entity must not be nil and must have a valid primary key value.
func (m *Model[T]) Update(ctx context.Context, entity *T) error {
	// Validate input
	if entity == nil {
		return ErrNilPointer
	}

	// Auto-update updated_at if it exists
	if fieldInfo, ok := m.modelInfo.Columns["updated_at"]; ok {
		val := reflect.ValueOf(entity).Elem()
		fieldVal := val.FieldByIndex(fieldInfo.Index)
		if fieldVal.CanSet() {
			// Use setFieldValue to handle type conversion safely
			_ = setFieldValue(fieldVal, time.Now())
		}
	}

	// Hooks
	if hook, ok := any(entity).(interface{ BeforeUpdate(context.Context) error }); ok {
		if err := hook.BeforeUpdate(ctx); err != nil {
			return err
		}
	}

	// Build Update Query
	var sets []string
	var values []any

	val := reflect.ValueOf(entity).Elem()

	for _, field := range m.modelInfo.Fields {
		if field.IsPrimary {
			continue
		}
		sets = append(sets, fmt.Sprintf("%s = ?", field.Column))
		values = append(values, val.FieldByName(field.Name).Interface())
	}

	var sb strings.Builder
	cteArgs := m.buildWithClause(&sb)

	sb.WriteString("UPDATE ")
	sb.WriteString(m.modelInfo.TableName)
	sb.WriteString(" SET ")
	sb.WriteString(strings.Join(sets, ", "))

	// If entity is passed, update that entity.
	// So add WHERE id = entity.ID

	pkVal := val.FieldByName(m.modelInfo.Columns[m.modelInfo.PrimaryKey].Name).Interface()
	sb.WriteString(fmt.Sprintf(" WHERE %s = ?", m.modelInfo.PrimaryKey))
	values = append(values, pkVal)

	query := sb.String()

	// args: CTE args + SET values + WHERE values
	allArgs := append(cteArgs, values...)

	var err error
	// Use prepared statement if caching is enabled
	if m.stmtCache != nil {
		var stmt *sql.Stmt
		stmt, err = m.prepareStmtForWrite(ctx, query)
		if err != nil {
			return WrapQueryError("PREPARE", query, values, err)
		}
		_, err = stmt.ExecContext(ctx, allArgs...)
	} else {
		_, err = m.queryerForWrite().ExecContext(ctx, query, allArgs...)
	}

	if err != nil {
		return WrapQueryError("UPDATE", query, values, err)
	}

	if hook, ok := any(entity).(interface{ AfterUpdate(context.Context) error }); ok {
		if err := hook.AfterUpdate(ctx); err != nil {
			return err
		}
	}

	return nil
}

// Delete deletes records matching the current query conditions.
// WARNING: Without WHERE conditions, this will delete ALL records in the table.
func (m *Model[T]) Delete(ctx context.Context) error {
	var sb strings.Builder
	cteArgs := m.buildWithClause(&sb)

	sb.WriteString("DELETE FROM ")
	sb.WriteString(m.modelInfo.TableName)
	m.buildWhereClause(&sb)

	query := sb.String()
	args := append(cteArgs, m.args...)

	var err error
	// Use prepared statement if caching is enabled
	if m.stmtCache != nil {
		var stmt *sql.Stmt
		stmt, err = m.prepareStmtForWrite(ctx, query)
		if err != nil {
			return WrapQueryError("PREPARE", query, m.args, err)
		}
		_, err = stmt.ExecContext(ctx, args...)
	} else {
		_, err = m.queryerForWrite().ExecContext(ctx, query, args...)
	}

	if err != nil {
		return WrapQueryError("DELETE", query, m.args, err)
	}
	return nil
}

// Exec executes the query (Raw or Builder) and returns the result.
func (m *Model[T]) Exec(ctx context.Context) (sql.Result, error) {
	if m.rawQuery != "" {
		return m.queryerForWrite().ExecContext(ctx, m.rawQuery, m.rawArgs...)
	}
	// For builder, we assume Delete or Update was called which executes immediately.
	// But if user wants to build a custom query?
	// Usually Exec is used with Raw.
	return nil, ErrRequiresRawQuery
}

// CreateMany inserts multiple records in a single query.
func (m *Model[T]) CreateMany(ctx context.Context, entities []*T) error {
	if len(entities) == 0 {
		return nil
	}

	// 2. Build Query
	var columns []string
	var placeholders []string

	// Determine columns from first entity (or model info)
	// We use modelInfo fields.
	// We assume all entities have same structure (they do, they are *T).

	// We need to identify which columns to insert.
	// We skip AutoIncrement PK if zero.

	// Prepare columns list
	var fieldsToInsert []string // Field names in struct

	val0 := reflect.ValueOf(entities[0]).Elem()
	for _, field := range m.modelInfo.Fields {
		if field.IsPrimary && field.IsAuto {
			fVal := val0.FieldByName(field.Name)
			if fVal.IsZero() {
				continue
			}
		}
		columns = append(columns, field.Column)
		fieldsToInsert = append(fieldsToInsert, field.Name)
		placeholders = append(placeholders, "?")
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(m.modelInfo.TableName)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(columns, ", "))
	sb.WriteString(") VALUES ")

	var args []any
	rowPlaceholder := "(" + strings.Join(placeholders, ", ") + ")"

	for i, entity := range entities {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(rowPlaceholder)

		val := reflect.ValueOf(entity).Elem()
		for _, fieldName := range fieldsToInsert {
			args = append(args, val.FieldByName(fieldName).Interface())
		}
	}

	// RETURNING ID?
	// Postgres supports returning IDs for multiple rows.
	sb.WriteString(" RETURNING " + m.modelInfo.PrimaryKey)

	query := sb.String()
	rows, err := m.queryerForWrite().QueryContext(ctx, query, args...)
	if err != nil {
		return WrapQueryError("INSERT", query, args, err)
	}
	defer rows.Close()

	// Scan IDs back
	i := 0
	pkField := m.modelInfo.Columns[m.modelInfo.PrimaryKey]

	for rows.Next() {
		if i >= len(entities) {
			break
		}
		entity := entities[i]
		val := reflect.ValueOf(entity).Elem()
		fVal := val.FieldByName(pkField.Name)

		if fVal.CanSet() {
			if err := rows.Scan(fVal.Addr().Interface()); err != nil {
				return err
			}
		}
		i++
	}

	return nil
}

// UpdateMany updates records matching the query with values.
func (m *Model[T]) UpdateMany(ctx context.Context, values map[string]any) error {
	if len(values) == 0 {
		return nil
	}

	// Auto-update updated_at if it exists and not provided
	if _, ok := m.modelInfo.Columns["updated_at"]; ok {
		if _, exists := values["updated_at"]; !exists {
			values["updated_at"] = time.Now()
		}
	}

	var sets []string
	var setArgs []any

	for k, v := range values {
		sets = append(sets, fmt.Sprintf("%s = ?", k))
		setArgs = append(setArgs, v)
	}

	var sb strings.Builder
	cteArgs := m.buildWithClause(&sb)

	sb.WriteString("UPDATE ")
	sb.WriteString(m.modelInfo.TableName)
	sb.WriteString(" SET ")
	sb.WriteString(strings.Join(sets, ", "))

	m.buildWhereClause(&sb)

	// Build args in correct order: CTE args, SET values, then WHERE values
	args := make([]any, 0, len(cteArgs)+len(setArgs)+len(m.args))
	args = append(args, cteArgs...)
	args = append(args, setArgs...)
	args = append(args, m.args...)

	query := sb.String()
	_, err := m.queryerForWrite().ExecContext(ctx, query, args...)
	if err != nil {
		return WrapQueryError("UPDATE", query, args, err)
	}
	return nil
}

// DeleteMany deletes records matching the query.
func (m *Model[T]) DeleteMany(ctx context.Context) error {
	return m.Delete(ctx)
}

// loadAccessors calls accessor methods (e.g., GetFullName) on model instances
// and populates the Attributes map with their return values.
// Accessor methods must start with "Get", take no arguments, and return a single value.
// The attribute key is the snake_case version of the method name with "Get" prefix removed.
// For example, GetFullName() -> attributes["full_name"].
func (m *Model[T]) loadAccessors(results []*T) {
	if len(results) == 0 {
		return
	}

	// Check if T has Attributes map[string]any
	// We inspect the first element
	val := reflect.ValueOf(results[0]).Elem()
	attrField := val.FieldByName("Attributes")

	if !attrField.IsValid() || attrField.Kind() != reflect.Map {
		return
	}

	// Find Accessor methods
	// We look for methods on *T or T that start with "Get" and take no args and return 1 value.
	typ := reflect.TypeOf(results[0])
	var accessors []reflect.Method

	for i := 0; i < typ.NumMethod(); i++ {
		method := typ.Method(i)
		if strings.HasPrefix(method.Name, "Get") && method.Type.NumIn() == 1 && method.Type.NumOut() == 1 {
			// Exclude "Get" itself or "GetX" where X is a field name?
			// Laravel uses "get...Attribute".
			// Requirements: "GetTypeLabel" -> "type_label"
			accessors = append(accessors, method)
		}
	}

	if len(accessors) == 0 {
		return
	}

	for _, res := range results {
		val := reflect.ValueOf(res).Elem()
		attrField := val.FieldByName("Attributes")
		if attrField.IsNil() {
			attrField.Set(reflect.MakeMap(attrField.Type()))
		}

		for _, method := range accessors {
			// Call method
			ret := method.Func.Call([]reflect.Value{reflect.ValueOf(res)})
			key := ToSnakeCase(strings.TrimPrefix(method.Name, "Get"))
			attrField.SetMapIndex(reflect.ValueOf(key), ret[0])
		}
	}
}
