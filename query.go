package zorm

import (
	"fmt"
	"reflect"
	"strings"
)

// Select specifies which columns to select.
func (m *Model[T]) Select(columns ...string) *Model[T] {
	m.columns = append(m.columns, columns...)
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
//	Where("column", value) -> column = ?
//	Where("column >", value) -> column > ?
//	Where(map[string]any{"name": "John", "age": 30}) -> name = ? AND age = ?
//	Where(&User{Name: "John"}) -> name = ?
//	Where(func(q *Model[T]) { ... }) -> nested group with parentheses
func (m *Model[T]) Where(query any, args ...any) *Model[T] {
	return m.addWhere("AND", query, args...)
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
			m.wheres = append(m.wheres, fmt.Sprintf("%s %s", typ, grouped))
			m.args = append(m.args, nested.args...)
		}
		return m
	}

	// 2. Handle Map
	if conditionMap, ok := query.(map[string]any); ok {
		for k, v := range conditionMap {
			m.wheres = append(m.wheres, fmt.Sprintf("%s %s = ?", typ, k))
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
		// Use ModelInfo if available and type matches?
		// Or just iterate fields and use ToSnakeCase?
		// Let's use ParseModelType to be safe and consistent
		info := ParseModelType(val.Type())
		for _, field := range info.Fields {
			fVal := val.FieldByName(field.Name)
			if !fVal.IsZero() {
				m.wheres = append(m.wheres, fmt.Sprintf("%s %s = ?", typ, field.Column))
				m.args = append(m.args, fVal.Interface())
			}
		}
		return m
	}

	// 4. Handle String
	queryStr, ok := query.(string)
	if !ok {
		return m
	}

	// If args provided and no ? and no operator, assume =
	if len(args) > 0 && !strings.Contains(queryStr, "?") {
		trimmed := strings.TrimSpace(queryStr)
		// Check for operator
		// We split by space to get the last part?
		// e.g. "age >" -> "age", ">"
		// "age" -> "age"
		parts := strings.Fields(trimmed)
		hasOperator := false
		if len(parts) > 1 {
			op := strings.ToUpper(parts[len(parts)-1])
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

	m.wheres = append(m.wheres, fmt.Sprintf("%s (%s)", typ, queryStr))
	m.args = append(m.args, args...)
	return m
}

// Chunk processes the results in chunks to save memory.
func (m *Model[T]) Chunk(size int, callback func([]*T) error) error {
	page := 1
	for {
		// Clone the builder to avoid modifying the original state permanently?
		// Or just modify limit/offset.
		// Since we are iterating, modifying m is fine if we reset or just keep moving.
		// But m.Get() uses m.limit/m.offset.

		// We need to preserve original conditions but update offset.
		offset := (page - 1) * size
		m.Limit(size).Offset(offset)

		results, err := m.Get()
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
func (m *Model[T]) WhereIn(column string, args []any) *Model[T] {
	if len(args) == 0 {
		// Optimization: 1=0 to return nothing
		m.wheres = append(m.wheres, "AND 1=0")
		return m
	}
	placeholders := make([]string, len(args))
	for i := range args {
		placeholders[i] = "?"
	}
	query := fmt.Sprintf("%s IN (%s)", column, strings.Join(placeholders, ","))
	m.wheres = append(m.wheres, fmt.Sprintf("AND (%s)", query))
	m.args = append(m.args, args...)
	return m
}

// OrderBy adds an ORDER BY clause.
func (m *Model[T]) OrderBy(column, direction string) *Model[T] {
	m.orderBys = append(m.orderBys, fmt.Sprintf("%s %s", column, direction))
	return m
}

// GroupBy adds a GROUP BY clause.
func (m *Model[T]) GroupBy(columns ...string) *Model[T] {
	m.groupBys = append(m.groupBys, columns...)
	return m
}

// GroupByRollup adds a GROUP BY ROLLUP clause.
func (m *Model[T]) GroupByRollup(columns ...string) *Model[T] {
	m.groupBys = append(m.groupBys, fmt.Sprintf("ROLLUP (%s)", strings.Join(columns, ", ")))
	return m
}

// GroupByCube adds a GROUP BY CUBE clause.
func (m *Model[T]) GroupByCube(columns ...string) *Model[T] {
	m.groupBys = append(m.groupBys, fmt.Sprintf("CUBE (%s)", strings.Join(columns, ", ")))
	return m
}

// GroupByGroupingSets adds a GROUP BY GROUPING SETS clause.
// Each slice in sets represents a grouping set.
// Empty slice represents empty grouping set ().
func (m *Model[T]) GroupByGroupingSets(sets ...[]string) *Model[T] {
	var setStrings []string
	for _, set := range sets {
		if len(set) == 0 {
			setStrings = append(setStrings, "()")
		} else {
			setStrings = append(setStrings, fmt.Sprintf("(%s)", strings.Join(set, ", ")))
		}
	}
	m.groupBys = append(m.groupBys, fmt.Sprintf("GROUPING SETS (%s)", strings.Join(setStrings, ", ")))
	return m
}

// Having adds a HAVING clause (used with GROUP BY).
func (m *Model[T]) Having(query string, args ...any) *Model[T] {
	// Similar to Where, but for HAVING
	if len(args) > 0 && !strings.Contains(query, "?") {
		trimmed := strings.TrimSpace(query)
		hasOperator := false
		operators := []string{"=", ">", "<", "LIKE", "ILIKE", "IS", "IN"}
		upper := strings.ToUpper(trimmed)
		for _, op := range operators {
			if strings.HasSuffix(upper, op) {
				hasOperator = true
				break
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
func (m *Model[T]) Paginate(page, perPage int) (*PaginationResult[T], error) {
	// 1. Count total
	// We need to clone the model to count without limit/offset?
	// Or just count current query ignoring limit/offset (which are not set yet usually).
	// But if user set OrderBy, Count() might fail in some DBs if strictly enforced? No, usually fine.

	// We need a separate count query.
	// Ideally, we clone the builder.
	// For now, let's assume Count() handles it or we do it manually.

	// Simple approach: Count() then Get().
	// But Count() executes.

	// We need to temporarily remove select columns for count?
	// m.Count() does `SELECT COUNT(*)`.

	total, err := m.Count()
	if err != nil {
		return nil, err
	}

	// 2. Apply Limit/Offset
	offset := (page - 1) * perPage
	m.Limit(perPage).Offset(offset)

	// 3. Get Data
	data, err := m.Get()
	if err != nil {
		return nil, err
	}

	lastPage := int(total) / perPage
	if int(total)%perPage != 0 {
		lastPage++
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
// Use this when you don't need the total count (e.g., "Load More" buttons).
// This is ~2x faster than Paginate() since it only runs 1 query.
func (m *Model[T]) SimplePaginate(page, perPage int) (*PaginationResult[T], error) {
	offset := (page - 1) * perPage
	m.Limit(perPage).Offset(offset)

	data, err := m.Get()
	if err != nil {
		return nil, err
	}

	return &PaginationResult[T]{
		Data:        data,
		Total:       -1, // -1 indicates count was skipped
		PerPage:     perPage,
		CurrentPage: page,
		LastPage:    -1, // Unknown without total
	}, nil
}

// WhereHas adds a WHERE EXISTS clause for a relation.
func (m *Model[T]) WhereHas(relation string, callback any) *Model[T] {
	return m.whereHasInternal(relation, callback)
}

func (m *Model[T]) whereHasInternal(relation string, subQuery any) *Model[T] {
	// 1. Get relation configuration
	var t T
	methodVal := reflect.ValueOf(t).MethodByName(relation)
	if !methodVal.IsValid() {
		// Silently skip if relation method doesn't exist
		// Could log or return error in strict mode
		return m
	}

	retVals := methodVal.Call(nil)
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
	foreignKey := valConfig.FieldByName("ForeignKey").String()
	if foreignKey == "" {
		// Default: parent_table_id (e.g., user_id)
		foreignKey = ToSnakeCase(m.modelInfo.Type.Name()) + "_id"
	}

	// 3. Get related model info
	relatedPtr := rel.NewRelated()
	relatedType := reflect.TypeOf(relatedPtr).Elem()
	relatedInfo := ParseModelType(relatedType)

	// 4. Build EXISTS subquery
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("EXISTS (SELECT 1 FROM %s WHERE %s.%s = %s.%s",
		relatedInfo.TableName,
		relatedInfo.TableName, foreignKey,
		m.modelInfo.TableName, m.modelInfo.PrimaryKey,
	))

	// 5. Apply additional constraints from callback
	if subQuery != nil {
		if wheres, args := extractQueryConstraints(subQuery); len(wheres) > 0 {
			for _, w := range wheres {
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
