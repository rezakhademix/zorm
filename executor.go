package zorm

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"reflect"
	"strings"
)

// queryer returns the query executor (tx or db).
func (m *Model[T]) queryer() interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
} {
	if m.tx != nil {
		return m.tx
	}
	return m.db
}

// Get executes the query and returns a slice of results.
func (m *Model[T]) Get() ([]*T, error) {
	query, args := m.buildSelectQuery()

	rows, err := m.queryer().QueryContext(m.ctx, query, args...)
	if err != nil {
		return nil, WrapQueryError("SELECT", query, args, err)
	}
	defer rows.Close()

	results, err := m.scanRows(rows)
	if err != nil {
		return nil, WrapQueryError("SCAN", query, args, err)
	}

	if err := m.loadRelations(results); err != nil {
		return nil, err
	}

	return results, nil
}

// First executes the query and returns the first result.
func (m *Model[T]) First() (*T, error) {
	m.Limit(1)
	results, err := m.Get()
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, ErrRecordNotFound
	}
	return results[0], nil
}

// Find finds a record by ID.
func (m *Model[T]) Find(id any) (*T, error) {
	return m.Where(m.modelInfo.PrimaryKey+" = ?", id).First()
}

// FindOrFail finds a record by ID or returns an error.
// In Go, this is identical to Find, but added for API parity.
func (m *Model[T]) FindOrFail(id any) (*T, error) {
	return m.Find(id)
}

// Pluck retrieves a single column's values from the result set.
func (m *Model[T]) Pluck(column string) ([]any, error) {
	// Select only that column
	m.columns = []string{column}

	query, args := m.buildSelectQuery()
	rows, err := m.queryer().QueryContext(m.ctx, query, args...)
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

	return results, rows.Err()
}

// Count returns the number of records matching the query.
func (m *Model[T]) Count() (int64, error) {
	// Backup limit/offset/order
	limit, offset := m.limit, m.offset
	orderBys := m.orderBys

	// Reset for count
	m.limit, m.offset = 0, 0
	m.orderBys = nil

	tableName := m.modelInfo.TableName
	var sb strings.Builder
	sb.WriteString("SELECT COUNT(*) FROM ")
	sb.WriteString(tableName)

	m.buildWhereClause(&sb)

	query := sb.String()
	args := m.args

	// Restore state
	m.limit, m.offset = limit, offset
	m.orderBys = orderBys

	var count int64
	err := m.queryer().QueryRowContext(m.ctx, query, args...).Scan(&count)

	return count, err
}

// CountOver returns count of records partitioned by the specified column.
// This uses window functions: COUNT(*) OVER (PARTITION BY column).
// Returns a map of column value -> count.
func (m *Model[T]) CountOver(column string) (map[any]int64, error) {
	// Build query: SELECT column, COUNT(*) OVER (PARTITION BY column) as count
	query := fmt.Sprintf("SELECT %s, COUNT(*) OVER (PARTITION BY %s) as count FROM %s",
		column, column, m.modelInfo.TableName)

	// Add WHERE clause
	var sb strings.Builder
	sb.WriteString(query)
	m.buildWhereClause(&sb)

	rows, err := m.queryer().QueryContext(m.ctx, sb.String(), m.args...)
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

// buildSelectQuery constructs the SQL SELECT statement.
func (m *Model[T]) buildSelectQuery() (string, []any) {
	if m.rawQuery != "" {
		return m.rawQuery, m.rawArgs
	}

	var sb strings.Builder
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

	m.buildWhereClause(&sb)

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

	if m.limit > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", m.limit))
	}

	if m.offset > 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", m.offset))
	}

	return sb.String(), m.args
}

func (m *Model[T]) buildWhereClause(sb *strings.Builder) {
	if len(m.wheres) > 0 {
		sb.WriteString(" WHERE 1=1 ") // Simplifies appending AND/OR
		for _, w := range m.wheres {
			sb.WriteString(" ")
			sb.WriteString(w)
		}
	}
}

// scanRows scans sql.Rows into a slice of *T.
func (m *Model[T]) scanRows(rows *sql.Rows) ([]*T, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []*T

	// Pre-calculate field mapping for performance
	// We need to map DB column index -> Struct Field
	// But we don't know the column order until now.

	for rows.Next() {
		// Create new instance of T
		// T is a struct, so we need a pointer to it.
		// new(T) returns *T
		entity := new(T)
		val := reflect.ValueOf(entity).Elem()

		// Prepare scan destinations
		dest := make([]any, len(columns))

		for i, colName := range columns {
			// Find the field for this column
			if fieldInfo, ok := m.modelInfo.Columns[colName]; ok {
				// Get the field value
				fieldVal := val.FieldByName(fieldInfo.Name)

				// If field is a pointer, we can scan directly into it?
				// sql.Scan handles *int, *string etc.
				// If field is int, we need &int.

				if fieldVal.Kind() == reflect.Pointer {
					// Field is *Type.
					// Initialize it if nil? No, sql.Scan will set it?
					// Actually sql.Scan needs a pointer to the value.
					// If field is *string, we pass **string? No.
					// We need to pass a pointer to the field.
					// fieldVal.Addr().Interface() is **string.
					// sql.Scan can handle **string and will allocate *string if not nil.
					dest[i] = fieldVal.Addr().Interface()
				} else {
					// Field is value (e.g. string).
					// We pass &string.
					dest[i] = fieldVal.Addr().Interface()
				}
			} else {
				// Column not in struct, ignore
				var ignore any
				dest[i] = &ignore
			}
		}

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
func (m *Model[T]) Cursor() (*Cursor[T], error) {
	query, args := m.buildSelectQuery()
	rows, err := m.queryer().QueryContext(m.ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return &Cursor[T]{
		rows:  rows,
		model: m,
	}, nil
}

// Cursor[T] allows iterating over query results.
type Cursor[T any] struct {
	rows  *sql.Rows
	model *Model[T]
}

// Next prepares the next result row for reading with the Scan method.
func (c *Cursor[T]) Next() bool {
	return c.rows.Next()
}

// Scan scans the current row into a new entity.
func (c *Cursor[T]) Scan() (*T, error) {
	// We need to replicate scanRows logic for a single row
	columns, err := c.rows.Columns()
	if err != nil {
		return nil, err
	}

	entity := new(T)
	val := reflect.ValueOf(entity).Elem()
	dest := make([]any, len(columns))

	for i, colName := range columns {
		if fieldInfo, ok := c.model.modelInfo.Columns[colName]; ok {
			fieldVal := val.FieldByName(fieldInfo.Name)
			if fieldVal.Kind() == reflect.Ptr {
				dest[i] = fieldVal.Addr().Interface()
			} else {
				dest[i] = fieldVal.Addr().Interface()
			}
		} else {
			var ignore any
			dest[i] = &ignore
		}
	}

	if err := c.rows.Scan(dest...); err != nil {
		return nil, err
	}

	// Load Accessors
	c.model.loadAccessors([]*T{entity})

	// Load Relations
	if len(c.model.relations) > 0 {
		if err := c.model.loadRelations([]*T{entity}); err != nil {
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
func (m *Model[T]) FirstOrCreate(attributes map[string]any, values map[string]any) (*T, error) {
	// Build query from attributes
	q := m
	for k, v := range attributes {
		q = q.Where(k+" = ?", v)
	}

	result, err := q.First()
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

	if err := m.Create(entity); err != nil {
		return nil, err
	}
	return entity, nil
}

// UpdateOrCreate finds a record matching attributes and updates it with values, or creates it.
func (m *Model[T]) UpdateOrCreate(attributes map[string]any, values map[string]any) (*T, error) {
	// Build query from attributes
	q := m
	for k, v := range attributes {
		q = q.Where(k+" = ?", v)
	}

	result, err := q.First()
	if err == nil && result != nil {
		// Found, update
		if err := fillStruct(result, values); err != nil {
			return nil, err
		}
		// We need to update only the changed fields? Or all values?
		// Update() updates all fields of the struct currently.
		if err := m.Update(result); err != nil {
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

	if err := m.Create(entity); err != nil {
		return nil, err
	}
	return entity, nil
}

// scanRowsDynamic scans rows into a slice of pointers to structs defined by modelInfo.
func (m *Model[T]) scanRowsDynamic(rows *sql.Rows, modelInfo *ModelInfo) ([]any, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []any

	for rows.Next() {
		// Create new instance of the struct type
		// modelInfo.Type is the struct type (e.g. User)
		// We need *User
		val := reflect.New(modelInfo.Type) // *User
		elem := val.Elem()                 // User

		dest := make([]any, len(columns))

		for i, colName := range columns {
			if fieldInfo, ok := modelInfo.Columns[colName]; ok {
				fieldVal := elem.FieldByName(fieldInfo.Name)
				if fieldVal.Kind() == reflect.Ptr {
					dest[i] = fieldVal.Addr().Interface()
				} else {
					dest[i] = fieldVal.Addr().Interface()
				}
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
func (m *Model[T]) Create(entity *T) error {
	// 1. BeforeCreate Hook
	if hook, ok := any(entity).(interface{ BeforeCreate(context.Context) error }); ok {
		if err := hook.BeforeCreate(m.ctx); err != nil {
			return err
		}
	}

	// 2. Build Insert Query
	// We need to map struct fields to columns
	// We use m.modelInfo

	var columns []string
	var values []any
	var placeholders []string

	val := reflect.ValueOf(entity).Elem()

	for _, field := range m.modelInfo.Fields {
		// Skip ID if auto-increment and zero?
		// For now, let's assume if ID is 0/empty we skip it.
		if field.IsPrimary && field.IsAuto {
			// Check if zero
			fVal := val.FieldByName(field.Name)
			if fVal.IsZero() {
				continue
			}
		}

		columns = append(columns, field.Column)
		values = append(values, val.FieldByName(field.Name).Interface())
		placeholders = append(placeholders, "?")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		m.modelInfo.TableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	// 3. Execute
	// If Postgres, we might want RETURNING id.
	// For generic SQL, we use LastInsertId (not supported by PG).

	query += " RETURNING " + m.modelInfo.PrimaryKey

	var id any
	// We need to scan the ID back into the entity
	err := m.queryer().QueryRowContext(m.ctx, query, values...).Scan(&id)
	if err != nil {
		return err
	}

	// Set ID back to entity
	// Find PK field
	if pkField, ok := m.modelInfo.Columns[m.modelInfo.PrimaryKey]; ok {
		fVal := val.FieldByName(pkField.Name)
		if fVal.CanSet() {
			// Convert id to field type if needed
			// Scan already does this if we passed &id?
			// No, we scanned into generic `id`.
			// Better: Scan directly into the field.
			// But we need a pointer to the field.
			// fVal.Addr().Interface()

			// Let's re-run QueryRow with direct scan
		}
	}

	// Actually, let's just scan into the field directly.
	if pkField, ok := m.modelInfo.Columns[m.modelInfo.PrimaryKey]; ok {
		fVal := val.FieldByName(pkField.Name)
		if fVal.CanSet() {
			err = m.queryer().QueryRowContext(m.ctx, query, values...).Scan(fVal.Addr().Interface())
			if err != nil {
				return err
			}
		}
	}

	// 4. AfterCreate Hook
	if hook, ok := any(entity).(interface{ AfterCreate(context.Context) error }); ok {
		if err := hook.AfterCreate(m.ctx); err != nil {
			return err
		}
	}

	return nil
}

// Update updates records.
func (m *Model[T]) Update(entity *T) error {
	// Hooks
	if hook, ok := any(entity).(interface{ BeforeUpdate(context.Context) error }); ok {
		if err := hook.BeforeUpdate(m.ctx); err != nil {
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
	sb.WriteString("UPDATE ")
	sb.WriteString(m.modelInfo.TableName)
	sb.WriteString(" SET ")
	sb.WriteString(strings.Join(sets, ", "))

	// If entity is passed, update that entity.
	// So add WHERE id = entity.ID

	pkVal := val.FieldByName(m.modelInfo.Columns[m.modelInfo.PrimaryKey].Name).Interface()
	sb.WriteString(fmt.Sprintf(" WHERE %s = ?", m.modelInfo.PrimaryKey))
	values = append(values, pkVal)

	_, err := m.queryer().ExecContext(m.ctx, sb.String(), values...)
	if err != nil {
		return err
	}

	if hook, ok := any(entity).(interface{ AfterUpdate(context.Context) error }); ok {
		if err := hook.AfterUpdate(m.ctx); err != nil {
			return err
		}
	}

	return nil
}

// Delete deletes records.
func (m *Model[T]) Delete() error {
	var sb strings.Builder
	sb.WriteString("DELETE FROM ")
	sb.WriteString(m.modelInfo.TableName)
	m.buildWhereClause(&sb)

	_, err := m.queryer().ExecContext(m.ctx, sb.String(), m.args...)
	return err
}

// Exec executes the query (Raw or Builder) and returns the result.
func (m *Model[T]) Exec() (sql.Result, error) {
	if m.rawQuery != "" {
		return m.queryer().ExecContext(m.ctx, m.rawQuery, m.rawArgs...)
	}
	// For builder, we assume Delete or Update was called which executes immediately.
	// But if user wants to build a custom query?
	// Usually Exec is used with Raw.
	return nil, ErrRequiresRawQuery
}

// CreateMany inserts multiple records in a single query.
func (m *Model[T]) CreateMany(entities []*T) error {
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

	rows, err := m.queryer().QueryContext(m.ctx, sb.String(), args...)
	if err != nil {
		return err
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
func (m *Model[T]) UpdateMany(values map[string]any) error {
	if len(values) == 0 {
		return nil
	}

	var sets []string
	var args []any

	for k, v := range values {
		sets = append(sets, fmt.Sprintf("%s = ?", k))
		args = append(args, v)
	}

	var sb strings.Builder
	sb.WriteString("UPDATE ")
	sb.WriteString(m.modelInfo.TableName)
	sb.WriteString(" SET ")
	sb.WriteString(strings.Join(sets, ", "))

	m.buildWhereClause(&sb)

	// Append where args
	args = append(args, m.args...)

	_, err := m.queryer().ExecContext(m.ctx, sb.String(), args...)
	return err
}

// DeleteMany deletes records matching the query.
func (m *Model[T]) DeleteMany() error {
	return m.Delete()
}

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
