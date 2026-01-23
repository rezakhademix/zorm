package zorm

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// Select specifies which columns to select.
// Column names are validated to prevent SQL injection.
func (m *Model[T]) Select(columns ...string) *Model[T] {
	for _, col := range columns {
		if err := ValidateColumnName(col); err != nil {
			// In strict mode, we could return an error
			// For now, skip invalid columns to maintain API compatibility
			continue
		}
		m.columns = append(m.columns, col)
	}
	return m
}

// Distinct adds DISTINCT to the SELECT clause to return only unique rows.
func (m *Model[T]) Distinct() *Model[T] {
	m.distinct = true

	return m
}

// DistinctBy adds DISTINCT ON (columns) to the SELECT clause.
// This is a PostgreSQL-specific feature that returns the first row of each set of rows
// where the given columns match.
func (m *Model[T]) DistinctBy(columns ...string) *Model[T] {
	m.distinctOn = append(m.distinctOn, columns...)

	return m
}

// Raw sets a raw SQL query and arguments.
func (m *Model[T]) Raw(query string, args ...any) *Model[T] {
	m.rawQuery = query
	m.rawArgs = args

	return m
}

// Where adds a WHERE clause.
// Supports multiple forms:
//
//	Where("column", value) -> column = ? (converted to $n at execution)
//	Where("column", ">", value) -> column > ?
//	Where(map[string]any{"name": "John", "age": 30}) -> name = ? AND age = ?
//	Where(&User{Name: "John"}) -> name = ?
//	Where(func(q *Model[T]) { ... }) -> nested group with parentheses
func (m *Model[T]) Where(query any, args ...any) *Model[T] {
	var (
		operator string
		value    any
	)

	switch len(args) {
	case 0:
		return m.addWhere("AND", query)
	case 1:
		value = args[0]
		operator = "="

		sb := GetStringBuilder()
		sb.WriteString(query.(string))
		sb.WriteByte(' ')
		sb.WriteString(operator)
		result := sb.String()
		PutStringBuilder(sb)
		return m.addWhere("AND", result, value)
	case 2:
		operator = fmt.Sprint(args[0])
		value = args[1]

		sb := GetStringBuilder()
		sb.WriteString(query.(string))
		sb.WriteByte(' ')
		sb.WriteString(operator)
		result := sb.String()
		PutStringBuilder(sb)
		return m.addWhere("AND", result, value)
	default:
		return m
	}
}

// OrWhere adds an OR WHERE clause.
func (m *Model[T]) OrWhere(query any, args ...any) *Model[T] {
	return m.addWhere("OR", query, args...)
}

func (m *Model[T]) addWhere(typ string, query any, args ...any) *Model[T] {
	// 1. Handle Callback
	if callback, ok := query.(func(*Model[T])); ok {
		nested := &Model[T]{
			ctx:       m.ctx,
			db:        m.db,
			tx:        m.tx,
			modelInfo: m.modelInfo,
		}
		callback(nested)
		if len(nested.wheres) > 0 {
			// Strip prefixes from nested wheres
			var conditions []string
			for _, w := range nested.wheres {
				w = strings.TrimSpace(w)
				w = strings.TrimPrefix(w, "AND ")
				w = strings.TrimPrefix(w, "OR ")
				conditions = append(conditions, w)
			}
			grouped := "(" + strings.Join(conditions, " ") + ")"
			m.wheres = append(m.wheres, typ+" "+grouped)
			m.args = append(m.args, nested.args...)
		}
		return m
	}

	// 2. Handle Map
	if conditionMap, ok := query.(map[string]any); ok {
		for k, v := range conditionMap {
			if err := ValidateColumnName(k); err != nil {
				continue
			}
			m.wheres = append(m.wheres, typ+" "+k+" = ?")
			m.args = append(m.args, v)
		}
		return m
	}

	// 3. Handle Struct
	val := reflect.ValueOf(query)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() == reflect.Struct {
		// Performance optimization: reuse ModelInfo if the struct type matches
		var info *ModelInfo
		if val.Type() == m.modelInfo.Type {
			info = m.modelInfo
		} else {
			info = ParseModelType(val.Type())
		}
		for _, field := range info.Fields {
			fVal := val.FieldByName(field.Name)
			if !fVal.IsZero() {
				if err := ValidateColumnName(field.Column); err != nil {
					continue
				}
				m.wheres = append(m.wheres, typ+" "+field.Column+" = ?")
				m.args = append(m.args, val.FieldByIndex(field.Index).Interface())
			}
		}
		return m
	}

	// 4. Handle String
	queryStr, ok := query.(string)
	if !ok {
		return m
	}

	if len(args) > 0 && !strings.Contains(queryStr, "?") {
		trimmed := strings.TrimSpace(queryStr)
		// Check for operator
		parts := strings.Fields(trimmed)
		hasOperator := false
		if len(parts) > 1 {
			op := strings.ToUpper(parts[1])
			operators := map[string]bool{
				"=": true, ">": true, "<": true, ">=": true, "<=": true,
				"LIKE": true, "ILIKE": true, "IS": true, "IN": true, "<>": true, "!=": true,
			}
			if operators[op] {
				hasOperator = true
			}
		}

		if hasOperator {
			queryStr = trimmed + " ?"
		} else {
			queryStr = trimmed + " = ?"
		}
	}

	m.wheres = append(m.wheres, typ+" "+queryStr)
	m.args = append(m.args, args...)
	return m
}

// WhereNull adds an AND condition that checks whether the given column is NULL.
// Column names are validated to prevent SQL injection.
//
// Example:
//
//	Model[User]().WhereNull("deleted_at")
//	// WHERE deleted_at IS NULL
func (m *Model[T]) WhereNull(column string) *Model[T] {
	if err := ValidateColumnName(column); err != nil {
		return m // Skip invalid column names
	}
	m.wheres = append(m.wheres, "AND "+column+" IS NULL")
	return m
}

// OrWhereNull adds an OR condition that checks whether the given column is NULL.
// Column names are validated to prevent SQL injection.
//
// Example:
//
//	Model[User]().OrWhereNull("deleted_at")
//	// OR deleted_at IS NULL
func (m *Model[T]) OrWhereNull(column string) *Model[T] {
	if err := ValidateColumnName(column); err != nil {
		return m // Skip invalid column names
	}
	m.wheres = append(m.wheres, "OR "+column+" IS NULL")
	return m
}

// WhereNotNull adds an AND condition that checks whether the given column is NOT NULL.
// Column names are validated to prevent SQL injection.
//
// Example:
//
//	Model[User]().WhereNotNull("verified_at")
//	// WHERE verified_at IS NOT NULL
func (m *Model[T]) WhereNotNull(column string) *Model[T] {
	if err := ValidateColumnName(column); err != nil {
		return m // Skip invalid column names
	}
	m.wheres = append(m.wheres, "AND "+column+" IS NOT NULL")
	return m
}

// OrWhereNotNull adds an OR condition that checks whether the given column is NOT NULL.
// Column names are validated to prevent SQL injection.
//
// Example:
//
//	Model[User]().OrWhereNotNull("verified_at")
//	// OR verified_at IS NOT NULL
func (m *Model[T]) OrWhereNotNull(column string) *Model[T] {
	if err := ValidateColumnName(column); err != nil {
		return m // Skip invalid column names
	}
	m.wheres = append(m.wheres, "OR "+column+" IS NOT NULL")
	return m
}

// Chunk processes the results in chunks to save memory.
// Uses Clone() for each iteration to avoid mutating the original query state.
func (m *Model[T]) Chunk(ctx context.Context, size int, callback func([]*T) error) error {
	page := 1
	for {
		// Clone to avoid mutating the original model's limit/offset
		q := m.Clone()
		offset := (page - 1) * size
		q.limit = size
		q.offset = offset

		results, err := q.Get(ctx)
		if err != nil {
			return err
		}

		if len(results) == 0 {
			break
		}

		if err := callback(results); err != nil {
			return err
		}

		if len(results) < size {
			break
		}

		page++
	}
	return nil
}

// WhereIn adds a WHERE IN clause.
// Column names are validated to prevent SQL injection.
func (m *Model[T]) WhereIn(column string, args []any) *Model[T] {
	if err := ValidateColumnName(column); err != nil {
		return m // Skip invalid column names
	}
	if len(args) == 0 {
		// Optimization: 1=0 to return nothing
		m.wheres = append(m.wheres, "AND 1=0")
		return m
	}
	// Optimized placeholder building using strings.Builder
	var sb strings.Builder
	sb.WriteString(column)
	sb.WriteString(" IN (")
	for i := range args {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('?')
	}
	sb.WriteByte(')')
	m.wheres = append(m.wheres, "AND "+sb.String())
	m.args = append(m.args, args...)
	return m
}

// OrderBy adds an ORDER BY clause.
// Column names are validated to prevent SQL injection.
func (m *Model[T]) OrderBy(column, direction string) *Model[T] {
	if err := ValidateColumnName(column); err != nil {
		return m // Skip invalid column names
	}
	// Validate direction is only ASC or DESC
	dir := strings.ToUpper(strings.TrimSpace(direction))
	if dir != "ASC" && dir != "DESC" {
		dir = "DESC" // Default to DESC if invalid
	}

	sb := GetStringBuilder()
	sb.WriteString(column)
	sb.WriteByte(' ')
	sb.WriteString(dir)
	m.orderBys = append(m.orderBys, sb.String())
	PutStringBuilder(sb)
	return m
}

// GroupBy adds a GROUP BY clause.
// Column names are validated to prevent SQL injection.
func (m *Model[T]) GroupBy(columns ...string) *Model[T] {
	for _, col := range columns {
		if err := ValidateColumnName(col); err != nil {
			continue // Skip invalid column names
		}
		m.groupBys = append(m.groupBys, col)
	}
	return m
}

// GroupByRollup adds a GROUP BY ROLLUP clause.
// Column names are validated to prevent SQL injection.
func (m *Model[T]) GroupByRollup(columns ...string) *Model[T] {
	var validCols []string
	for _, col := range columns {
		if err := ValidateColumnName(col); err != nil {
			continue // Skip invalid column names
		}
		validCols = append(validCols, col)
	}
	if len(validCols) > 0 {
		sb := GetStringBuilder()
		sb.WriteString("ROLLUP (")
		sb.WriteString(strings.Join(validCols, ", "))
		sb.WriteByte(')')
		m.groupBys = append(m.groupBys, sb.String())
		PutStringBuilder(sb)
	}
	return m
}

// GroupByCube adds a GROUP BY CUBE clause.
// Column names are validated to prevent SQL injection.
func (m *Model[T]) GroupByCube(columns ...string) *Model[T] {
	var validCols []string
	for _, col := range columns {
		if err := ValidateColumnName(col); err != nil {
			continue // Skip invalid column names
		}
		validCols = append(validCols, col)
	}
	if len(validCols) > 0 {
		sb := GetStringBuilder()
		sb.WriteString("CUBE (")
		sb.WriteString(strings.Join(validCols, ", "))
		sb.WriteByte(')')
		m.groupBys = append(m.groupBys, sb.String())
		PutStringBuilder(sb)
	}
	return m
}

// GroupByGroupingSets adds a GROUP BY GROUPING SETS clause.
// Each slice in sets represents a grouping set.
// Empty slice represents empty grouping set ().
// Column names are validated to prevent SQL injection.
func (m *Model[T]) GroupByGroupingSets(sets ...[]string) *Model[T] {
	var setStrings []string
	for _, set := range sets {
		if len(set) == 0 {
			setStrings = append(setStrings, "()")
		} else {
			var validCols []string
			for _, col := range set {
				if err := ValidateColumnName(col); err != nil {
					continue // Skip invalid column names
				}
				validCols = append(validCols, col)
			}
			if len(validCols) > 0 {
				setSb := GetStringBuilder()
				setSb.WriteByte('(')
				setSb.WriteString(strings.Join(validCols, ", "))
				setSb.WriteByte(')')
				setStrings = append(setStrings, setSb.String())
				PutStringBuilder(setSb)
			}
		}
	}
	if len(setStrings) > 0 {
		sb := GetStringBuilder()
		sb.WriteString("GROUPING SETS (")
		sb.WriteString(strings.Join(setStrings, ", "))
		sb.WriteByte(')')
		m.groupBys = append(m.groupBys, sb.String())
		PutStringBuilder(sb)
	}
	return m
}

// Having adds a HAVING clause (used with GROUP BY).
// The query string is validated to prevent SQL injection by checking for dangerous patterns.
// Use parameterized values (?) for user input.
// Example: Having("COUNT(*) > ?", 5)
func (m *Model[T]) Having(query string, args ...any) *Model[T] {
	// Validate query to prevent SQL injection
	if err := ValidateRawQuery(query); err != nil {
		return m // Skip invalid queries
	}

	// Similar to Where, but for HAVING
	if len(args) > 0 && !strings.Contains(query, "?") {
		trimmed := strings.TrimSpace(query)
		hasOperator := false
		operators := []string{"=", ">", "<", "LIKE", "ILIKE", "IS", "IN"}
		upper := strings.ToUpper(trimmed)
		for _, op := range operators {
			if strings.HasSuffix(upper, op) {
				hasOperator = true
			}
		}
		if hasOperator {
			query = trimmed + " ?"
		} else {
			query = trimmed + " = ?"
		}
	}
	m.havings = append(m.havings, query)
	m.args = append(m.args, args...)
	return m
}

// Latest adds an ORDER BY column DESC clause. Defaults to "created_at".
func (m *Model[T]) Latest(columns ...string) *Model[T] {
	col := "created_at"
	if len(columns) > 0 {
		col = columns[0]
	}
	return m.OrderBy(col, "DESC")
}

// Oldest adds an ORDER BY column ASC clause. Defaults to "created_at".
func (m *Model[T]) Oldest(columns ...string) *Model[T] {
	col := "created_at"
	if len(columns) > 0 {
		col = columns[0]
	}
	return m.OrderBy(col, "ASC")
}

// Limit sets the LIMIT clause.
func (m *Model[T]) Limit(n int) *Model[T] {
	m.limit = n
	return m
}

// Offset sets the OFFSET clause.
func (m *Model[T]) Offset(n int) *Model[T] {
	m.offset = n
	return m
}

// Scope applies a function to the query builder.
// Useful for reusable query logic (Scopes).
func (m *Model[T]) Scope(fn func(*Model[T]) *Model[T]) *Model[T] {
	return fn(m)
}

// PaginationResult holds pagination metadata and data.
type PaginationResult[T any] struct {
	Data        []*T  `json:"data"`
	Total       int64 `json:"total"`
	PerPage     int   `json:"per_page"`
	CurrentPage int   `json:"current_page"`
	LastPage    int   `json:"last_page"`
}

// Paginate executes the query with pagination.
// Uses Clone() to avoid mutating the original query state.
// If page is less than 1, it defaults to 1.
// If perPage is less than 1, it defaults to 15.
func (m *Model[T]) Paginate(ctx context.Context, page, perPage int) (*PaginationResult[T], error) {
	if page < 1 {
		page = 1
	}
	if perPage < 1 {
		perPage = 15
	}

	total, err := m.Count(ctx)
	if err != nil {
		return nil, err
	}

	// Clone to avoid mutating the original model's limit/offset
	q := m.Clone()
	offset := (page - 1) * perPage
	q.limit = perPage
	q.offset = offset

	data, err := q.Get(ctx)
	if err != nil {
		return nil, err
	}

	lastPage := int(total) / perPage
	if int(total)%perPage != 0 {
		lastPage++
	}

	if lastPage == 0 {
		lastPage = 1
	}

	return &PaginationResult[T]{
		Data:        data,
		Total:       total,
		PerPage:     perPage,
		CurrentPage: page,
		LastPage:    lastPage,
	}, nil
}

// SimplePaginate executes the query with pagination but skips the count query.
// Uses Clone() to avoid mutating the original query state.
// Use this when you don't need the total count (e.g., "Load More" buttons).
// This is ~2x faster than Paginate() since it only runs 1 query.
// If page is less than 1, it defaults to 1.
// If perPage is less than 1, it defaults to 15.
func (m *Model[T]) SimplePaginate(ctx context.Context, page, perPage int) (*PaginationResult[T], error) {
	if page < 1 {
		page = 1
	}
	if perPage < 1 {
		perPage = 15
	}

	// Clone to avoid mutating the original model's limit/offset
	q := m.Clone()
	offset := (page - 1) * perPage
	q.limit = perPage
	q.offset = offset

	data, err := q.Get(ctx)
	if err != nil {
		return nil, err
	}

	return &PaginationResult[T]{
		Data:        data,
		Total:       -1, // -1 indicates count was skipped
		PerPage:     perPage,
		CurrentPage: page,
		LastPage:    -1, // indicates total was skipped
	}, nil
}

// WhereHas adds a WHERE EXISTS clause for a relation.
func (m *Model[T]) WhereHas(relation string, callback any) *Model[T] {
	return m.whereHasInternal(relation, callback)
}

func (m *Model[T]) whereHasInternal(relation string, subQuery any) *Model[T] {
	// 1. Get relation configuration
	var t T
	modelType := reflect.TypeOf(t)
	ptrType := reflect.PointerTo(modelType)

	// Try finding method on ptrType or modelType
	method, ok := ptrType.MethodByName(relation)
	if !ok {
		method, ok = modelType.MethodByName(relation)
		if !ok {
			// Try with "Relation" suffix
			method, ok = ptrType.MethodByName(relation + "Relation")
			if !ok {
				method, ok = modelType.MethodByName(relation + "Relation")
				if !ok {
					return m // Relation method not found
				}
			}
		}
	}

	// For whereHasInternal, we need to call the method.
	// We use the zero value of the model.
	res0 := reflect.ValueOf(t)
	// If method is on pointer and res0 is not, we might need a pointer.
	// But reflect.Value.MethodByName handles this if we use a pointer value.
	ptrValue := reflect.New(modelType)
	var methodVal reflect.Value
	if method.Type.NumIn() > 0 && method.Type.In(0).Kind() == reflect.Pointer {
		methodVal = ptrValue.MethodByName(method.Name)
	} else {
		methodVal = res0.MethodByName(method.Name)
	}

	if !methodVal.IsValid() {
		// Fallback: try calling via method.Func with receiver
		if method.Type.NumIn() > 0 {
			if method.Type.In(0).Kind() == reflect.Pointer {
				retVals := method.Func.Call([]reflect.Value{ptrValue})
				return m.applyWhereHas(retVals, subQuery)
			}
			retVals := method.Func.Call([]reflect.Value{res0})
			return m.applyWhereHas(retVals, subQuery)
		}
		return m
	}

	retVals := methodVal.Call(nil)
	return m.applyWhereHas(retVals, subQuery)
}

func (m *Model[T]) applyWhereHas(retVals []reflect.Value, subQuery any) *Model[T] {
	if len(retVals) == 0 {
		return m
	}

	relConfig := retVals[0].Interface()
	rel, ok := relConfig.(Relation)
	if !ok {
		return m
	}

	// 2. Determine foreign key
	valConfig := reflect.ValueOf(relConfig)
	if valConfig.Kind() == reflect.Ptr {
		valConfig = valConfig.Elem()
	}
	foreignKey := valConfig.FieldByName("ForeignKey").String()
	if foreignKey == "" {
		// Default: parent_table_id (e.g., user_id)
		foreignKey = ToSnakeCase(m.modelInfo.Type.Name()) + "_id"
	}

	// 3. Get related model info
	relatedPtr := rel.NewRelated()
	relatedType := reflect.TypeOf(relatedPtr).Elem()
	relatedInfo := ParseModelType(relatedType)

	// Extract Table Override
	var relTable string
	if overrider, ok := relConfig.(TableOverrider); ok {
		relTable = overrider.GetOverrideTable()
	}
	tableName := relatedInfo.TableName
	if relTable != "" {
		tableName = relTable
	}

	// 4. Build EXISTS subquery
	var sb strings.Builder
	sb.WriteString("EXISTS (SELECT 1 FROM ")
	sb.WriteString(tableName)
	sb.WriteString(" WHERE ")
	sb.WriteString(tableName)
	sb.WriteByte('.')
	sb.WriteString(foreignKey)
	sb.WriteString(" = ")
	sb.WriteString(m.TableName())
	sb.WriteByte('.')
	sb.WriteString(m.modelInfo.PrimaryKey)

	// 5. Apply additional constraints from callback
	if subQuery != nil {
		var constraints []string
		var args []any

		// Check if it's a function
		fnVal := reflect.ValueOf(subQuery)
		if fnVal.Kind() == reflect.Func {
			// Create a new model for the related type
			relatedModel := rel.NewModel(m.ctx, m.db)
			if relatedModel != nil {
				fnVal.Call([]reflect.Value{reflect.ValueOf(relatedModel)})
				// Extract constraints from the populated model
				constraints, args = extractQueryConstraints(relatedModel)
			}
		} else {
			constraints, args = extractQueryConstraints(subQuery)
		}

		if len(constraints) > 0 {
			for _, w := range constraints {
				sb.WriteString(" ")
				sb.WriteString(w)
			}
			m.args = append(m.args, args...)
		}
	}

	sb.WriteString(")")
	m.wheres = append(m.wheres, "AND "+sb.String())

	return m
}

// extractQueryConstraints safely extracts WHERE clauses and args from a query builder.
// Returns empty slices if extraction fails.
func extractQueryConstraints(query any) ([]string, []any) {
	if query == nil {
		return nil, nil
	}

	sqVal := reflect.ValueOf(query)
	if !sqVal.IsValid() {
		return nil, nil
	}

	// Try to get GetWheres and GetArgs methods
	getWheres := sqVal.MethodByName("GetWheres")
	getArgs := sqVal.MethodByName("GetArgs")

	if !getWheres.IsValid() || !getArgs.IsValid() {
		return nil, nil
	}

	// Call methods and extract results
	wheresResult := getWheres.Call(nil)
	argsResult := getArgs.Call(nil)

	if len(wheresResult) == 0 || len(argsResult) == 0 {
		return nil, nil
	}

	// Type assert with safety checks
	wheres, ok1 := wheresResult[0].Interface().([]string)
	args, ok2 := argsResult[0].Interface().([]any)

	if !ok1 || !ok2 {
		return nil, nil
	}

	return wheres, args
}

// GetWheres returns the where clauses.
func (m *Model[T]) GetWheres() []string {
	return m.wheres
}

// GetArgs returns the arguments.
func (m *Model[T]) GetArgs() []any {
	return m.args
}

// With adds relations to eager load.
// Multiple relation names can be specified, including nested relations.
//
// Examples:
//
//	With("Posts")                    // Single relation
//	With("Posts", "Comments")        // Multiple relations
//	With("Posts.Comments")           // Nested relation
func (m *Model[T]) With(relations ...string) *Model[T] {
	m.relations = append(m.relations, relations...)
	return m
}

// WithCallback adds a relation with a callback to apply constraints.
// The callback receives a query builder for the related model and can apply
// filters, ordering, limits, etc.
//
// Example:
//
//	WithCallback("Posts", func(q *Model[Post]) {
//	    q.Where("published", true).OrderBy("created_at", "DESC").Limit(10)
//	})
func (m *Model[T]) WithCallback(relation string, callback any) *Model[T] {
	if m.relationCallbacks == nil {
		m.relationCallbacks = make(map[string]any)
	}
	m.relations = append(m.relations, relation)
	m.relationCallbacks[relation] = callback
	return m
}

// WithMorph adds a polymorphic relation to eager load with type-specific constraints.
// typeMap: map[string][]string{"events": {"Calendar"}, "posts": {"Author"}}
func (m *Model[T]) WithMorph(relation string, typeMap map[string][]string) *Model[T] {
	if m.morphRelations == nil {
		m.morphRelations = make(map[string]map[string][]string)
	}
	m.relations = append(m.relations, relation)
	m.morphRelations[relation] = typeMap
	return m
}

// WithCTE adds a Common Table Expression (CTE) to the query.
// query can be a string or a *Model[T].
func (m *Model[T]) WithCTE(name string, query any) *Model[T] {
	m.ctes = append(m.ctes, CTE{
		Name:  name,
		Query: query,
	})
	return m
}

// Lock adds a locking clause to the SELECT query.
// Common modes: "UPDATE", "NO KEY UPDATE", "SHARE", "KEY SHARE"
// This will generate: SELECT ... FOR [mode]
func (m *Model[T]) Lock(mode string) *Model[T] {
	m.lockMode = mode
	return m
}

// WhereFullText adds a full-text search condition using tsvector and tsquery.
// Uses default 'english' configuration and plainto_tsquery for user-friendly search.
// Column names are validated to prevent SQL injection.
// Example: WhereFullText("content", "search terms")
// Generates: WHERE to_tsvector('english', content) @@ plainto_tsquery('english', ?)
func (m *Model[T]) WhereFullText(column, searchText string) *Model[T] {
	if err := ValidateColumnName(column); err != nil {
		return m // Skip invalid column names
	}

	sb := GetStringBuilder()
	sb.WriteString("AND to_tsvector('english', ")
	sb.WriteString(column)
	sb.WriteString(") @@ plainto_tsquery('english', ?)")
	m.wheres = append(m.wheres, sb.String())
	PutStringBuilder(sb)
	m.args = append(m.args, searchText)
	return m
}

// WhereFullTextWithConfig adds a full-text search condition with a custom text search configuration.
// Column names and config are validated to prevent SQL injection.
// Example: WhereFullTextWithConfig("content", "search terms", "spanish")
// Generates: WHERE to_tsvector('spanish', content) @@ plainto_tsquery('spanish', ?)
func (m *Model[T]) WhereFullTextWithConfig(column, searchText, config string) *Model[T] {
	if err := ValidateColumnName(column); err != nil {
		return m // Skip invalid column names
	}
	if err := ValidateColumnName(config); err != nil {
		return m // Skip invalid config names
	}

	sb := GetStringBuilder()
	sb.WriteString("AND to_tsvector('")
	sb.WriteString(config)
	sb.WriteString("', ")
	sb.WriteString(column)
	sb.WriteString(") @@ plainto_tsquery('")
	sb.WriteString(config)
	sb.WriteString("', ?)")
	m.wheres = append(m.wheres, sb.String())
	PutStringBuilder(sb)
	m.args = append(m.args, searchText)
	return m
}

// WhereTsVector adds a full-text search condition on a pre-computed tsvector column.
// This is more efficient when you have an indexed tsvector column.
// Column names are validated to prevent SQL injection.
// Example: WhereTsVector("search_vector", "fat & rat")
// Generates: WHERE search_vector @@ to_tsquery('english', ?)
func (m *Model[T]) WhereTsVector(tsvectorColumn, tsquery string) *Model[T] {
	if err := ValidateColumnName(tsvectorColumn); err != nil {
		return m // Skip invalid column names
	}

	sb := GetStringBuilder()
	sb.WriteString("AND ")
	sb.WriteString(tsvectorColumn)
	sb.WriteString(" @@ to_tsquery('english', ?)")
	m.wheres = append(m.wheres, sb.String())
	PutStringBuilder(sb)
	m.args = append(m.args, tsquery)

	return m
}

// WherePhraseSearch adds an exact phrase search condition.
// Uses phraseto_tsquery which preserves word order.
// Column names are validated to prevent SQL injection.
// Example: WherePhraseSearch("content", "fat cat")
// Generates: WHERE to_tsvector('english', content) @@ phraseto_tsquery('english', ?)
func (m *Model[T]) WherePhraseSearch(column, phrase string) *Model[T] {
	if err := ValidateColumnName(column); err != nil {
		return m // Skip invalid column names
	}

	sb := GetStringBuilder()
	sb.WriteString("AND to_tsvector('english', ")
	sb.WriteString(column)
	sb.WriteString(") @@ phraseto_tsquery('english', ?)")
	m.wheres = append(m.wheres, sb.String())
	PutStringBuilder(sb)
	m.args = append(m.args, phrase)
	
	return m
}

// UsePrimary forces the next query to use the primary database connection.
// This is useful when you need to read from primary for consistency,
// such as immediately after a write operation.
// Example: m.UsePrimary().Get()
func (m *Model[T]) UsePrimary() *Model[T] {
	m.forcePrimary = true
	m.forceReplica = -1
	return m
}

// UseReplica forces the next query to use a specific replica by index.
// This is useful for testing or when you want to target a specific replica.
// Example: m.UseReplica(0).Get()
func (m *Model[T]) UseReplica(index int) *Model[T] {
	m.forcePrimary = false
	m.forceReplica = index
	return m
}

// Print returns the SQL query and arguments that would be executed without running it.
// This is useful for debugging and logging the generated SQL.
// Example:
//
//	sql, args := m.Where("status", "active").Limit(10).Print()
//	fmt.Println(sql, args)
//
// Output: "SELECT * FROM users WHERE 1=1 AND (status = $1) LIMIT 10" [active]
func (m *Model[T]) Print() (string, []any) {
	var query string
	var args []any

	// If raw query is set, return it
	if m.rawQuery != "" {
		query = m.rawQuery
		args = m.rawArgs
	} else {
		// Otherwise, build the SELECT query
		query, args = m.buildSelectQuery()
	}

	return rebind(query), args
}

// rebind replaces ? placeholders with $1, $2, etc.
func rebind(query string) string {
	sb := GetStringBuilder()
	defer PutStringBuilder(sb)

	// Pre-allocate assuming similar length plus some extra for placeholders
	sb.Grow(len(query) + 16)

	questionMarkCount := 0
	inQuote := false

	// ... existing loop ...
	for i := 0; i < len(query); i++ {
		c := query[i]
		if c == '\'' {
			inQuote = !inQuote
		}

		if c == '?' && !inQuote {
			questionMarkCount++
			sb.WriteByte('$')
			sb.WriteString(strconv.Itoa(questionMarkCount))
		} else {
			sb.WriteByte(c)
		}
	}
	return strings.Clone(sb.String())
}
