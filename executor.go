package zorm

import (
	"container/list"
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"maps"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

// queryer returns the appropriate query executor based on transaction state and resolver configuration.
// If a transaction is active (m.tx != nil), it returns the transaction executor.
// If a resolver is configured via GetGlobalResolver(), it routes based on forcePrimary/forceReplica flags.
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
	if resolver := GetGlobalResolver(); resolver != nil {
		return m.resolveDB(resolver)
	}

	// Fallback to model or global DB
	if m.db != nil {
		return m.db
	}
	return GlobalDB
}

// resolveDB determines which database connection to use based on resolver configuration.
func (m *Model[T]) resolveDB(resolver *DBResolver) *sql.DB {
	// Manual override: force primary
	if m.forcePrimary {
		return resolver.Primary()
	}

	// Manual override: force specific replica
	if m.forceReplica >= 0 {
		db := resolver.ReplicaAt(m.forceReplica)
		if db != nil {
			return db
		}
		// Fallback to load-balanced replica if index is invalid
	}

	// Auto-select replica (load balanced)
	// For read operations, this will be called by executor
	return resolver.Replica()
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
	if resolver := GetGlobalResolver(); resolver != nil {
		return resolver.Primary()
	}

	// Fallback to model or global DB
	if m.db != nil {
		return m.db
	}
	return GlobalDB
}

// prepareStmtWithQueryer is the internal implementation for statement preparation.
// It takes a queryer interface to allow reuse between read and write operations.
func (m *Model[T]) prepareStmtWithQueryer(ctx context.Context, query string, q interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}) (*sql.Stmt, func(), error) {
	// If caching is not enabled, prepare directly
	if m.stmtCache == nil {
		var stmt *sql.Stmt
		var err error
		// We need the underlying *sql.DB or *sql.Tx to prepare
		if db, ok := q.(*sql.DB); ok {
			stmt, err = db.PrepareContext(ctx, query)
		} else if tx, ok := q.(*sql.Tx); ok {
			stmt, err = tx.PrepareContext(ctx, query)
		} else {
			return nil, nil, fmt.Errorf("unable to prepare statement: invalid queryer type")
		}

		if err != nil {
			return nil, nil, err
		}
		// Return a release function that closes the statement
		return stmt, func() { stmt.Close() }, nil
	}

	// Try to get from cache
	if stmt, release := m.stmtCache.Get(query); stmt != nil {
		return stmt, release, nil
	}

	// Not in cache, prepare it
	var stmt *sql.Stmt
	var err error

	if db, ok := q.(*sql.DB); ok {
		stmt, err = db.PrepareContext(ctx, query)
	} else if tx, ok := q.(*sql.Tx); ok {
		stmt, err = tx.PrepareContext(ctx, query)
	} else {
		return nil, nil, fmt.Errorf("unable to prepare statement: invalid queryer type")
	}

	if err != nil {
		return nil, nil, err
	}

	// Store in cache and get with incremented ref count atomically
	// This avoids race conditions where the statement could be evicted between Put and Get
	cachedStmt, release := m.stmtCache.PutAndGet(query, stmt)
	return cachedStmt, release, nil
}

// prepareStmt returns a prepared statement for the given query.
// If statement caching is enabled (m.stmtCache != nil), it attempts to:
// 1. Retrieve the statement from cache
// 2. If not found, prepare the statement and cache it
// If caching is not enabled, it prepares the statement directly without caching.
//
// Returns the statement and a release function. The caller MUST call the release function
// when finished using the statement.
func (m *Model[T]) prepareStmt(ctx context.Context, query string) (*sql.Stmt, func(), error) {
	return m.prepareStmtWithQueryer(ctx, query, m.queryer())
}

// prepareStmtForWrite returns a prepared statement for write operations.
// Similar to prepareStmt but uses queryerForWrite to ensure primary database is used.
func (m *Model[T]) prepareStmtForWrite(ctx context.Context, query string) (*sql.Stmt, func(), error) {
	return m.prepareStmtWithQueryer(ctx, query, m.queryerForWrite())
}

// Get executes the query and returns a slice of results.
func (m *Model[T]) Get(ctx context.Context) ([]*T, error) {
	query, args := m.buildSelectQuery()

	var rows *sql.Rows
	var err error

	// Use prepared statement if caching is enabled
	if m.stmtCache != nil {
		var stmt *sql.Stmt
		var release func()
		stmt, release, err = m.prepareStmt(ctx, rebind(query))
		if err != nil {
			return nil, WrapQueryError("PREPARE", query, args, err)
		}
		defer release()

		rows, err = stmt.QueryContext(ctx, args...)
	} else {
		rows, err = m.queryer().QueryContext(ctx, rebind(query), args...)
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
// Uses Clone() to avoid mutating the original query state.
func (m *Model[T]) First(ctx context.Context) (*T, error) {
	// Clone to avoid mutating the original model's limit
	q := m.Clone()
	q.limit = 1
	results, err := q.Get(ctx)
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
	return m.Where(m.modelInfo.PrimaryKey, id).First(ctx)
}

// FindOrFail finds a record by ID or returns an error.
// In Go, this is identical to Find, but added for API parity.
func (m *Model[T]) FindOrFail(ctx context.Context, id any) (*T, error) {
	return m.Find(ctx, id)
}

// Pluck retrieves a single column's values from the result set.
// Column names are validated to prevent SQL injection.
// This method is safe for concurrent use - it clones the model before modification.
func (m *Model[T]) Pluck(ctx context.Context, column string) ([]any, error) {
	if err := ValidateColumnName(column); err != nil {
		return nil, err
	}

	// Clone to avoid mutating shared state (thread-safe)
	q := m.Clone()
	q.columns = []string{column}

	query, args := q.buildSelectQuery()

	rows, err := q.queryer().QueryContext(ctx, rebind(query), args...)
	if err != nil {
		return nil, WrapQueryError("SELECT", query, args, err)
	}
	defer rows.Close()

	// Pre-allocate results slice based on limit or default capacity
	initialCap := q.limit
	if initialCap <= 0 {
		initialCap = 64 // Default capacity for unbounded queries
	}
	results := make([]any, 0, initialCap)

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
// This method is safe for concurrent use - it clones the model before modification.
func (m *Model[T]) Count(ctx context.Context) (int64, error) {
	// Clone to avoid mutating shared state (thread-safe)
	q := m.Clone()
	q.limit, q.offset = 0, 0
	q.orderBys = nil

	tableName := q.TableName()
	var sb strings.Builder
	cteArgs := q.buildWithClause(&sb)

	sb.WriteString("SELECT COUNT(*) FROM ")
	sb.WriteString(tableName)

	q.buildWhereClause(&sb)

	query := sb.String()
	args := append(cteArgs, q.args...)

	var count int64
	var err error

	// Use prepared statement if caching is enabled
	if q.stmtCache != nil {
		var stmt *sql.Stmt
		var release func()
		stmt, release, err = q.prepareStmt(ctx, rebind(query))
		if err != nil {
			return 0, WrapQueryError("PREPARE", query, args, err)
		}
		defer release()

		err = stmt.QueryRowContext(ctx, args...).Scan(&count)
	} else {
		err = q.queryer().QueryRowContext(ctx, rebind(query), args...).Scan(&count)
	}

	if err != nil {
		return 0, WrapQueryError("COUNT", query, args, err)
	}

	return count, nil
}

// Exists checks if any record matches the query conditions.
// It uses "SELECT 1 FROM table WHERE conditions LIMIT 1" for efficiency.
// This method is safe for concurrent use - it clones the model before modification.
func (m *Model[T]) Exists(ctx context.Context) (bool, error) {
	// Clone to avoid mutating shared state (thread-safe)
	q := m.Clone()
	q.limit = 1
	q.offset = 0
	q.orderBys = nil

	tableName := q.TableName()
	var sb strings.Builder
	cteArgs := q.buildWithClause(&sb)

	sb.WriteString("SELECT 1 FROM ")
	sb.WriteString(tableName)

	q.buildWhereClause(&sb)
	sb.WriteString(" LIMIT 1")

	query := sb.String()
	args := append(cteArgs, q.args...)

	var exists int
	var err error

	// Use prepared statement if caching is enabled
	if q.stmtCache != nil {
		var stmt *sql.Stmt
		var release func()
		stmt, release, err = q.prepareStmt(ctx, rebind(query))
		if err != nil {
			return false, WrapQueryError("PREPARE", query, args, err)
		}
		defer release()

		err = stmt.QueryRowContext(ctx, args...).Scan(&exists)
	} else {
		err = q.queryer().QueryRowContext(ctx, rebind(query), args...).Scan(&exists)
	}

	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, WrapQueryError("EXISTS", query, args, err)
	}

	return true, nil
}

// Sum calculates the sum of a column.
// Returns 0 if no rows match or the sum is null.
// Column names are validated to prevent SQL injection.
// This method is safe for concurrent use - it clones the model before modification.
func (m *Model[T]) Sum(ctx context.Context, column string) (float64, error) {
	if err := ValidateColumnName(column); err != nil {
		return 0, err
	}

	// Clone to avoid mutating shared state (thread-safe)
	q := m.Clone()
	q.limit, q.offset = 0, 0
	q.orderBys = nil

	tableName := q.TableName()
	var sb strings.Builder
	cteArgs := q.buildWithClause(&sb)

	sb.WriteString("SELECT SUM(")
	sb.WriteString(column)
	sb.WriteString(") FROM ")
	sb.WriteString(tableName)

	q.buildWhereClause(&sb)

	query := sb.String()
	args := append(cteArgs, q.args...)

	var result sql.NullFloat64
	var err error

	// Use prepared statement if caching is enabled
	if q.stmtCache != nil {
		var stmt *sql.Stmt
		var release func()
		stmt, release, err = q.prepareStmt(ctx, query)
		if err != nil {
			return 0, WrapQueryError("PREPARE", query, args, err)
		}
		defer release()

		err = stmt.QueryRowContext(ctx, args...).Scan(&result)
	} else {
		err = q.queryer().QueryRowContext(ctx, query, args...).Scan(&result)
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
// Column names are validated to prevent SQL injection.
// This method is safe for concurrent use - it clones the model before modification.
func (m *Model[T]) Avg(ctx context.Context, column string) (float64, error) {
	if err := ValidateColumnName(column); err != nil {
		return 0, err
	}

	// Clone to avoid mutating shared state (thread-safe)
	q := m.Clone()
	q.limit, q.offset = 0, 0
	q.orderBys = nil

	tableName := q.TableName()
	var sb strings.Builder
	cteArgs := q.buildWithClause(&sb)

	sb.WriteString("SELECT AVG(")
	sb.WriteString(column)
	sb.WriteString(") FROM ")
	sb.WriteString(tableName)

	q.buildWhereClause(&sb)

	query := sb.String()
	args := append(cteArgs, q.args...)

	var result sql.NullFloat64
	var err error

	// Use prepared statement if caching is enabled
	if q.stmtCache != nil {
		var stmt *sql.Stmt
		var release func()
		stmt, release, err = q.prepareStmt(ctx, query)
		if err != nil {
			return 0, WrapQueryError("PREPARE", query, args, err)
		}
		defer release()

		err = stmt.QueryRowContext(ctx, args...).Scan(&result)
	} else {
		err = q.queryer().QueryRowContext(ctx, query, args...).Scan(&result)
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
// Column names are validated to prevent SQL injection.
func (m *Model[T]) CountOver(ctx context.Context, column string) (map[any]int64, error) {
	if err := ValidateColumnName(column); err != nil {
		return nil, err
	}

	// Build query: SELECT column, COUNT(*) OVER (PARTITION BY column) as count
	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(column)
	sb.WriteString(", COUNT(*) OVER (PARTITION BY ")
	sb.WriteString(column)
	sb.WriteString(") as count FROM ")
	sb.WriteString(m.TableName())

	// Add WHERE clause
	m.buildWhereClause(&sb)

	rows, err := m.queryer().QueryContext(ctx, rebind(sb.String()), m.args...)
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
	sb.WriteString(m.TableName())

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

// columnMappingCache caches column-to-field mappings per query signature.
// Key format: "typeName:col1,col2,col3"
// Note: We use type name (not table name) because different Go types can map to the same table
// but have different field definitions.
// Uses sharded LRU cache to prevent unbounded memory growth.
var columnMappingCache = newColumnCache(1000) // Cache up to 1000 mappings

// columnCache is a sharded LRU cache for column mappings.
type columnCache struct {
	shards   [64]*columnCacheShard
	capacity int
}

type columnCacheShard struct {
	mu       sync.Mutex
	items    map[string]*columnCacheEntry
	lruList  *list.List
	capacity int
}

type columnCacheEntry struct {
	key     string
	value   []*FieldInfo
	element *list.Element // Reference to list element for O(1) MoveToFront
}

func newColumnCache(capacity int) *columnCache {
	shardCapacity := capacity / 64
	if shardCapacity < 1 {
		shardCapacity = 1
	}

	c := &columnCache{capacity: capacity}
	for i := 0; i < 64; i++ {
		c.shards[i] = &columnCacheShard{
			items:    make(map[string]*columnCacheEntry),
			lruList:  list.New(),
			capacity: shardCapacity,
		}
	}
	return c
}

func (c *columnCache) getShard(key string) *columnCacheShard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return c.shards[h.Sum32()%64]
}

func (c *columnCache) Load(key string) ([]*FieldInfo, bool) {
	shard := c.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if entry, ok := shard.items[key]; ok {
		shard.lruList.MoveToFront(entry.element) // Update LRU order
		return entry.value, true
	}
	return nil, false
}

func (c *columnCache) Store(key string, value []*FieldInfo) {
	shard := c.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, exists := shard.items[key]; exists {
		return // Already exists
	}

	// Evict if at capacity
	if len(shard.items) >= shard.capacity {
		if back := shard.lruList.Back(); back != nil {
			entry := back.Value.(*columnCacheEntry)
			delete(shard.items, entry.key)
			shard.lruList.Remove(back)
		}
	}

	entry := &columnCacheEntry{key: key, value: value}
	entry.element = shard.lruList.PushFront(entry)
	shard.items[key] = entry
}

// mapColumns maps database columns to struct field info.
// Returns a slice where each element corresponds to the column at that index.
// Uses caching to avoid repeated lookups for the same column set.
func (m *Model[T]) mapColumns(columns []string) []*FieldInfo {
	// Build cache key using type name (not table name) to avoid collisions
	// when different Go types map to the same database table
	key := m.modelInfo.Type.String() + ":" + strings.Join(columns, ",")

	// Check cache first
	if cached, ok := columnMappingCache.Load(key); ok {
		return cached
	}

	// Build mapping
	fields := make([]*FieldInfo, len(columns))
	for i, col := range columns {
		fields[i] = m.modelInfo.Columns[col]
	}

	// Cache and return
	columnMappingCache.Store(key, fields)
	return fields
}

// fillScanDestinations creates scan destinations for sql.Rows.Scan based on pre-calculated field mapping.
// It reuses the dest slice to avoid allocations per row.
func (m *Model[T]) fillScanDestinations(fields []*FieldInfo, val reflect.Value, dest []any) {
	for i, f := range fields {
		if f != nil {
			dest[i] = val.FieldByIndex(f.Index).Addr().Interface()
		} else {
			var ignore any
			dest[i] = &ignore
		}
	}
}

// scanRows scans sql.Rows into a slice of *T.
// It uses pre-calculated field mapping and reused destination slice for performance.
// Automatically tracks original values for dirty checking.
// If a tracking scope is configured, entities are registered with the scope.
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

	// Prepare mapping and destination slice once
	fields := m.mapColumns(columns)
	dest := make([]any, len(columns))

	for rows.Next() {
		// Create new instance of T
		entity := new(T)
		val := reflect.ValueOf(entity).Elem()

		// Fill scan destinations
		m.fillScanDestinations(fields, val, dest)

		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}

		// Track originals for dirty tracking (with optional scope)
		trackOriginalsWithScope(entity, m.modelInfo, m.trackingScope)

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
	rows, err := m.queryer().QueryContext(ctx, rebind(query), args...)
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
	columns []string     // Cached column names
	fields  []*FieldInfo // Cached field mapping
	dest    []any        // Cached scan destination slice
}

// Next prepares the next result row for reading with the Scan method.
func (c *Cursor[T]) Next() bool {
	return c.rows.Next()
}

// Scan scans the current row into a new entity.
// Automatically tracks original values for dirty checking.
// If a tracking scope is configured, entities are registered with the scope.
func (c *Cursor[T]) Scan(ctx context.Context) (*T, error) {
	// Cache columns and mapping on first call
	if c.columns == nil {
		var err error
		c.columns, err = c.rows.Columns()
		if err != nil {
			return nil, err
		}
		// Init cache
		c.fields = c.model.mapColumns(c.columns)
		c.dest = make([]any, len(c.columns))
	}

	entity := new(T)
	val := reflect.ValueOf(entity).Elem()

	// Use helper to fill destinations
	c.model.fillScanDestinations(c.fields, val, c.dest)

	if err := c.rows.Scan(c.dest...); err != nil {
		return nil, err
	}

	// Track originals for dirty tracking (with optional scope)
	trackOriginalsWithScope(entity, c.model.modelInfo, c.model.trackingScope)

	// Load Accessors (using single-entity version to avoid slice allocation)
	c.model.loadAccessorsSingle(entity)

	// Load Relations if any are configured
	if len(c.model.relations) > 0 {
		if err := c.model.loadRelations(ctx, []*T{entity}); err != nil {
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
	q := m.Clone()
	for k, v := range attributes {
		q = q.Where(k, v)
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
	q := m.Clone()
	for k, v := range attributes {
		q = q.Where(k, v)
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
// Optimized to cache field mapping and reuse destination slices.
func (m *Model[T]) scanRowsDynamic(rows *sql.Rows, modelInfo *ModelInfo) ([]any, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Pre-calculate field mapping once (similar to scanRows)
	fields := make([]*FieldInfo, len(columns))
	for i, colName := range columns {
		fields[i] = modelInfo.Columns[colName]
	}

	// Pre-allocate results with reasonable capacity
	results := make([]any, 0, 64)

	// Reusable destination slice - content will be overwritten each row
	dest := make([]any, len(columns))

	for rows.Next() {
		// Create new instance of the struct type
		val := reflect.New(modelInfo.Type) // *User
		elem := val.Elem()                 // User

		// Fill scan destinations using cached field mapping
		for i, f := range fields {
			if f != nil {
				dest[i] = elem.FieldByIndex(f.Index).Addr().Interface()
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
	numFields := len(m.modelInfo.Fields)
	columns := make([]string, 0, numFields)
	values := make([]any, 0, numFields)

	val := reflect.ValueOf(entity).Elem()

	for _, field := range m.modelInfo.Fields {
		fVal := val.FieldByIndex(field.Index)
		// Skip auto-increment primary key if zero
		if field.IsPrimary && field.IsAuto {
			if fVal.IsZero() {
				continue
			}
		}

		columns = append(columns, field.Column)
		values = append(values, fVal.Interface())
	}

	sb := GetStringBuilder()
	sb.WriteString("INSERT INTO ")
	sb.WriteString(m.modelInfo.TableName)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(columns, ", "))
	sb.WriteString(") VALUES (")
	writePlaceholdersWithSeparator(sb, len(columns), ", ")
	sb.WriteString(") RETURNING ")
	sb.WriteString(m.modelInfo.PrimaryKey)
	query := sb.String()
	PutStringBuilder(sb)

	// 3. Execute and scan ID directly into the primary key field
	pkField, ok := m.modelInfo.Columns[m.modelInfo.PrimaryKey]
	if !ok {
		return fmt.Errorf("primary key field %s not found in model", m.modelInfo.PrimaryKey)
	}

	fVal := val.FieldByIndex(pkField.Index)
	if !fVal.CanSet() {
		return fmt.Errorf("cannot set primary key field %s", pkField.Name)
	}

	var err error
	// Use prepared statement if caching is enabled
	if m.stmtCache != nil {
		var stmt *sql.Stmt
		var release func()
		stmt, release, err = m.prepareStmtForWrite(ctx, rebind(query))
		if err != nil {
			return WrapQueryError("PREPARE", query, values, err)
		}
		defer release()

		err = stmt.QueryRowContext(ctx, values...).Scan(fVal.Addr().Interface())
	} else {
		err = m.queryerForWrite().QueryRowContext(ctx, rebind(query), values...).Scan(fVal.Addr().Interface())
	}

	if err != nil {
		return WrapQueryError("INSERT", query, values, err)
	}

	// Track the newly created entity so it can be used with dirty tracking
	trackOriginalsWithScope(entity, m.modelInfo, m.trackingScope)

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
	numFields := len(m.modelInfo.Fields)
	sets := make([]string, 0, numFields)
	values := make([]any, 0, numFields+1) // +1 for PK value

	val := reflect.ValueOf(entity).Elem()

	for _, field := range m.modelInfo.Fields {
		if field.IsPrimary {
			continue
		}

		// Skip omitted columns
		if m.omitColumns != nil && m.omitColumns[field.Column] {
			continue
		}

		sets = append(sets, field.Column+" = ?")
		values = append(values, val.FieldByIndex(field.Index).Interface())
	}

	var sb strings.Builder
	cteArgs := m.buildWithClause(&sb)

	sb.WriteString("UPDATE ")
	sb.WriteString(m.modelInfo.TableName)
	sb.WriteString(" SET ")
	sb.WriteString(strings.Join(sets, ", "))

	// If entity is passed, update that entity.
	// So add WHERE id = entity.ID

	pkField := m.modelInfo.Columns[m.modelInfo.PrimaryKey]
	pkVal := val.FieldByIndex(pkField.Index).Interface()
	sb.WriteString(" WHERE ")
	sb.WriteString(m.modelInfo.PrimaryKey)
	sb.WriteString(" = ?")
	values = append(values, pkVal)

	query := sb.String()

	// args: CTE args + SET values + WHERE values
	allArgs := append(cteArgs, values...)

	var err error
	// Use prepared statement if caching is enabled
	if m.stmtCache != nil {
		var stmt *sql.Stmt
		var release func()
		stmt, release, err = m.prepareStmtForWrite(ctx, rebind(query))
		if err != nil {
			return WrapQueryError("PREPARE", query, values, err)
		}
		defer release()

		_, err = stmt.ExecContext(ctx, allArgs...)
	} else {
		_, err = m.queryerForWrite().ExecContext(ctx, rebind(query), allArgs...)
	}

	if err != nil {
		return WrapQueryError("UPDATE", query, values, err)
	}

	// Sync originals after successful update to mark entity as clean
	syncOriginals(entity, m.modelInfo)

	if hook, ok := any(entity).(interface{ AfterUpdate(context.Context) error }); ok {
		if err := hook.AfterUpdate(ctx); err != nil {
			return err
		}
	}

	return nil
}

// UpdateColumns updates only the specified columns of the entity.
// This is useful when you want explicit control over which columns are updated.
//
// Example:
//
//	user.Name = "New Name"
//	user.Email = "new@email.com"
//	err := model.UpdateColumns(ctx, user, "name", "email")  // Only updates name and email
func (m *Model[T]) UpdateColumns(ctx context.Context, entity *T, columns ...string) error {
	if entity == nil {
		return ErrNilPointer
	}

	if len(columns) == 0 {
		return nil // Nothing to update
	}

	// Auto-update updated_at if it exists and not explicitly specified
	hasUpdatedAt := false
	for _, col := range columns {
		if col == "updated_at" {
			hasUpdatedAt = true
			break
		}
	}

	if !hasUpdatedAt {
		if fieldInfo, ok := m.modelInfo.Columns["updated_at"]; ok {
			val := reflect.ValueOf(entity).Elem()
			fieldVal := val.FieldByIndex(fieldInfo.Index)
			if fieldVal.CanSet() {
				_ = setFieldValue(fieldVal, time.Now())
				columns = append(columns, "updated_at")
			}
		}
	}

	// BeforeUpdate Hook
	if hook, ok := any(entity).(interface{ BeforeUpdate(context.Context) error }); ok {
		if err := hook.BeforeUpdate(ctx); err != nil {
			return err
		}
	}

	// Build UPDATE with specified columns
	var sets []string
	var values []any

	val := reflect.ValueOf(entity).Elem()

	for _, column := range columns {
		field, ok := m.modelInfo.Columns[column]
		if !ok || field.IsPrimary {
			continue
		}

		sets = append(sets, column+" = ?")
		values = append(values, val.FieldByIndex(field.Index).Interface())
	}

	if len(sets) == 0 {
		return nil
	}

	// Build query
	var sb strings.Builder
	cteArgs := m.buildWithClause(&sb)

	sb.WriteString("UPDATE ")
	sb.WriteString(m.modelInfo.TableName)
	sb.WriteString(" SET ")
	sb.WriteString(strings.Join(sets, ", "))

	// WHERE id = ?
	pkField := m.modelInfo.Columns[m.modelInfo.PrimaryKey]
	pkVal := val.FieldByIndex(pkField.Index).Interface()
	sb.WriteString(" WHERE ")
	sb.WriteString(m.modelInfo.PrimaryKey)
	sb.WriteString(" = ?")
	values = append(values, pkVal)

	query := sb.String()
	allArgs := append(cteArgs, values...)

	// Execute
	var err error
	if m.stmtCache != nil {
		var stmt *sql.Stmt
		var release func()
		stmt, release, err = m.prepareStmtForWrite(ctx, rebind(query))
		if err != nil {
			return WrapQueryError("PREPARE", query, values, err)
		}
		defer release()

		_, err = stmt.ExecContext(ctx, allArgs...)
	} else {
		_, err = m.queryerForWrite().ExecContext(ctx, rebind(query), allArgs...)
	}

	if err != nil {
		return WrapQueryError("UPDATE", query, values, err)
	}

	// Sync originals after successful update
	syncOriginals(entity, m.modelInfo)

	// AfterUpdate Hook
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
		var release func()
		stmt, release, err = m.prepareStmtForWrite(ctx, rebind(query))
		if err != nil {
			return WrapQueryError("PREPARE", query, m.args, err)
		}
		defer release()

		_, err = stmt.ExecContext(ctx, args...)
	} else {
		_, err = m.queryerForWrite().ExecContext(ctx, rebind(query), args...)
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
	numFields := len(m.modelInfo.Fields)
	columns := make([]string, 0, numFields)

	// We need to identify which columns to insert.
	// We skip AutoIncrement PK if zero.

	// Prepare columns list
	fieldsToInsert := make([][]int, 0, numFields) // Field indices in struct

	val0 := reflect.ValueOf(entities[0]).Elem()
	for _, field := range m.modelInfo.Fields {
		fVal := val0.FieldByIndex(field.Index)
		if field.IsPrimary && field.IsAuto {
			if fVal.IsZero() {
				continue
			}
		}
		columns = append(columns, field.Column)
		fieldsToInsert = append(fieldsToInsert, field.Index)
	}

	// Determine chunk size based on Postgres limit of 65535 parameters
	numColumns := len(columns)
	if numColumns == 0 {
		numColumns = 1 // Safety to avoid division by zero
	}

	chunkSize := 65535 / numColumns
	if chunkSize > 500 {
		chunkSize = 500 // Cap at reasonable batch size
	} else if chunkSize < 1 {
		chunkSize = 1
	}

	if len(entities) <= chunkSize {
		return m.createBatch(ctx, entities, columns, fieldsToInsert)
	}

	// Use a transaction for multiple chunks to ensure atomicity
	var tx *sql.Tx
	var err error
	var committed bool
	if m.tx == nil {
		db := m.db
		if db == nil {
			db = GlobalDB
		}
		if db == nil {
			return ErrNilDatabase
		}
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() {
			if !committed {
				tx.Rollback()
			}
		}()
	}

	// Execute in chunks
	for i := 0; i < len(entities); i += chunkSize {
		end := i + chunkSize
		if end > len(entities) {
			end = len(entities)
		}

		batch := entities[i:end]
		// Create a clone with the transaction for this batch
		batchModel := m.Clone()
		if tx != nil {
			batchModel.tx = tx
		}

		if err := batchModel.createBatch(ctx, batch, columns, fieldsToInsert); err != nil {
			return err
		}
	}

	if tx != nil {
		if err := tx.Commit(); err != nil {
			return err
		}
		committed = true
	}

	return nil
}

// createBatch performs a single batch insert query.
func (m *Model[T]) createBatch(ctx context.Context, entities []*T, columns []string, fieldsToInsert [][]int) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(m.TableName())
	sb.WriteString(" (")
	sb.WriteString(strings.Join(columns, ", "))
	sb.WriteString(") VALUES ")

	// Pre-allocate args slice with exact capacity
	args := make([]any, 0, len(entities)*len(fieldsToInsert))

	// Build row placeholder once: "(?, ?, ...)"
	var rowSb strings.Builder
	rowSb.WriteByte('(')
	writePlaceholdersWithSeparator(&rowSb, len(columns), ", ")
	rowSb.WriteByte(')')
	rowPlaceholder := rowSb.String()

	for i, entity := range entities {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(rowPlaceholder)

		val := reflect.ValueOf(entity).Elem()
		for _, fieldIndex := range fieldsToInsert {
			args = append(args, val.FieldByIndex(fieldIndex).Interface())
		}
	}

	// RETURNING ID?
	sb.WriteString(" RETURNING " + m.modelInfo.PrimaryKey)

	query := sb.String()
	rows, err := m.queryerForWrite().QueryContext(ctx, rebind(query), args...)
	if err != nil {
		return WrapQueryError("INSERT", query, args, err)
	}
	defer rows.Close()

	// Scan IDs back
	idx := 0
	pkField := m.modelInfo.Columns[m.modelInfo.PrimaryKey]

	for rows.Next() {
		if idx >= len(entities) {
			break
		}
		entity := entities[idx]
		val := reflect.ValueOf(entity).Elem()
		fVal := val.FieldByIndex(pkField.Index)

		if fVal.CanSet() {
			if err := rows.Scan(fVal.Addr().Interface()); err != nil {
				return err
			}
		}
		idx++
	}
	return rows.Err()
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
		if err := ValidateColumnName(k); err != nil {
			return err
		}

		setSb := GetStringBuilder()
		setSb.WriteString(k)
		setSb.WriteString(" = ?")
		sets = append(sets, setSb.String())
		PutStringBuilder(setSb)
		setArgs = append(setArgs, v)
	}

	var sb strings.Builder
	cteArgs := m.buildWithClause(&sb)

	sb.WriteString("UPDATE ")
	sb.WriteString(m.TableName())
	sb.WriteString(" SET ")
	sb.WriteString(strings.Join(sets, ", "))

	m.buildWhereClause(&sb)

	// Build args in correct order: CTE args, SET values, then WHERE values
	args := make([]any, 0, len(cteArgs)+len(setArgs)+len(m.args))
	args = append(args, cteArgs...)
	args = append(args, setArgs...)
	args = append(args, m.args...)

	query := sb.String()
	_, err := m.queryerForWrite().ExecContext(ctx, rebind(query), args...)
	if err != nil {
		return WrapQueryError("UPDATE", query, args, err)
	}
	return nil
}

// DeleteMany deletes records matching the query.
func (m *Model[T]) DeleteMany(ctx context.Context) error {
	return m.Delete(ctx)
}

// BulkInsert inserts multiple records using a single prepared statement.
// This is more efficient than CreateMany for scenarios where you need
// fine-grained control or want to handle errors per-entity.
// The prepared statement is reused for each entity, reducing preparation overhead.
//
// Example:
//
//	users := []*User{{Name: "Alice"}, {Name: "Bob"}, {Name: "Charlie"}}
//	err := model.BulkInsert(ctx, users)
func (m *Model[T]) BulkInsert(ctx context.Context, entities []*T) error {
	if len(entities) == 0 {
		return nil
	}

	// Determine columns from first entity
	var columns []string
	var fieldsToInsert []*FieldInfo

	val0 := reflect.ValueOf(entities[0]).Elem()
	for _, field := range m.modelInfo.Fields {
		fVal := val0.FieldByIndex(field.Index)
		if field.IsPrimary && field.IsAuto {
			if fVal.IsZero() {
				continue
			}
		}
		columns = append(columns, field.Column)
		fieldsToInsert = append(fieldsToInsert, field)
	}

	// Build INSERT query once
	sb := GetStringBuilder()
	sb.WriteString("INSERT INTO ")
	sb.WriteString(m.TableName())
	sb.WriteString(" (")
	sb.WriteString(strings.Join(columns, ", "))
	sb.WriteString(") VALUES (")
	for i := range columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('?')
	}
	sb.WriteString(") RETURNING ")
	sb.WriteString(m.modelInfo.PrimaryKey)
	insertQuery := rebind(sb.String())
	PutStringBuilder(sb)

	// Get database connection for preparing
	db := m.db
	if db == nil {
		db = GlobalDB
	}
	if db == nil {
		return ErrNilDatabase
	}

	// If in transaction, use transaction's prepare
	var stmt *sql.Stmt
	var err error
	if m.tx != nil {
		stmt, err = m.tx.PrepareContext(ctx, insertQuery)
	} else {
		stmt, err = db.PrepareContext(ctx, insertQuery)
	}
	if err != nil {
		return WrapQueryError("PREPARE", insertQuery, nil, err)
	}
	defer stmt.Close()

	// Get primary key field info
	pkField, ok := m.modelInfo.Columns[m.modelInfo.PrimaryKey]
	if !ok {
		return fmt.Errorf("primary key field %s not found in model", m.modelInfo.PrimaryKey)
	}

	// Pre-allocate args slice
	args := make([]any, len(fieldsToInsert))

	// Execute for each entity
	for _, entity := range entities {
		val := reflect.ValueOf(entity).Elem()

		// Extract values using cached field indices
		for i, field := range fieldsToInsert {
			args[i] = val.FieldByIndex(field.Index).Interface()
		}

		// Execute and scan returned ID
		fVal := val.FieldByIndex(pkField.Index)
		if fVal.CanSet() {
			err = stmt.QueryRowContext(ctx, args...).Scan(fVal.Addr().Interface())
			if err != nil {
				return WrapQueryError("INSERT", insertQuery, args, err)
			}
		} else {
			_, err = stmt.ExecContext(ctx, args...)
			if err != nil {
				return WrapQueryError("INSERT", insertQuery, args, err)
			}
		}
	}

	return nil
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

	// Use cached accessors from ModelInfo
	accessorIndices := m.modelInfo.Accessors
	if len(accessorIndices) == 0 {
		return
	}

	typ := reflect.TypeOf(results[0])

	type methodCache struct {
		method reflect.Method
		key    string
	}
	methods := make([]methodCache, len(accessorIndices))
	for i, idx := range accessorIndices {
		method := typ.Method(idx)
		methods[i] = methodCache{
			method: method,
			key:    ToSnakeCase(strings.TrimPrefix(method.Name, "Get")),
		}
	}

	callArgs := make([]reflect.Value, 1)

	for _, res := range results {
		resVal := reflect.ValueOf(res)
		elem := resVal.Elem()
		attrField := elem.FieldByName("Attributes")
		if attrField.IsNil() {
			attrField.Set(reflect.MakeMap(attrField.Type()))
		}

		// Update receiver for method calls
		callArgs[0] = resVal

		for _, mc := range methods {
			// Call method using cached method and reused args slice
			ret := mc.method.Func.Call(callArgs)
			attrField.SetMapIndex(reflect.ValueOf(mc.key), ret[0])
		}
	}
}

// loadAccessorsSingle processes Get* methods for a single entity and populates Attributes.
// This is an optimized version of loadAccessors that avoids slice allocation for single entities.
func (m *Model[T]) loadAccessorsSingle(entity *T) {
	if entity == nil {
		return
	}

	val := reflect.ValueOf(entity).Elem()
	attrField := val.FieldByName("Attributes")

	if !attrField.IsValid() || attrField.Kind() != reflect.Map {
		return
	}

	// Use cached accessors from ModelInfo
	accessorIndices := m.modelInfo.Accessors
	if len(accessorIndices) == 0 {
		return
	}

	if attrField.IsNil() {
		attrField.Set(reflect.MakeMap(attrField.Type()))
	}

	resVal := reflect.ValueOf(entity)
	typ := resVal.Type()
	callArgs := []reflect.Value{resVal}

	for _, idx := range accessorIndices {
		method := typ.Method(idx)
		key := ToSnakeCase(strings.TrimPrefix(method.Name, "Get"))
		ret := method.Func.Call(callArgs)
		attrField.SetMapIndex(reflect.ValueOf(key), ret[0])
	}
}
