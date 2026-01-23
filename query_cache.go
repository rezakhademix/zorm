package zorm

import (
	"strings"
	"sync"
)

// queryTemplateCache caches base query patterns to avoid rebuilding them.
// Keys are in the format "tableName:queryType" (e.g., "users:select", "users:insert").
var queryTemplateCache sync.Map

// QueryTemplateType represents the type of query template.
type QueryTemplateType string

const (
	QueryTemplateSelect QueryTemplateType = "select"
	QueryTemplateInsert QueryTemplateType = "insert"
	QueryTemplateUpdate QueryTemplateType = "update"
	QueryTemplateDelete QueryTemplateType = "delete"
)

// selectTemplate holds a cached SELECT query template.
type selectTemplate struct {
	baseQuery string   // "SELECT * FROM tableName"
	columns   []string // columns for the select
}

// insertTemplate holds a cached INSERT query template.
type insertTemplate struct {
	columns      []string // column names
	placeholders string   // "(?, ?, ?)"
	returning    string   // "RETURNING id"
}

// getCachedSelectBase returns the cached base SELECT query for a table.
// Returns empty string if not cached.
func getCachedSelectBase(tableName string) string {
	key := tableName + ":select"
	if cached, ok := queryTemplateCache.Load(key); ok {
		return cached.(string)
	}
	return ""
}

// setCachedSelectBase stores the base SELECT query for a table.
func setCachedSelectBase(tableName, query string) {
	key := tableName + ":select"
	queryTemplateCache.Store(key, query)
}

// getCachedInsertTemplate returns the cached INSERT template for a model.
// The key includes the table name and column count to handle different insert scenarios.
func getCachedInsertTemplate(tableName string, columnCount int) *insertTemplate {
	key := tableName + ":insert:" + string(rune(columnCount))
	if cached, ok := queryTemplateCache.Load(key); ok {
		return cached.(*insertTemplate)
	}
	return nil
}

// setCachedInsertTemplate stores the INSERT template for a model.
func setCachedInsertTemplate(tableName string, columnCount int, template *insertTemplate) {
	key := tableName + ":insert:" + string(rune(columnCount))
	queryTemplateCache.Store(key, template)
}

// buildSelectBase builds the base SELECT query: "SELECT * FROM tableName"
func (m *Model[T]) buildSelectBase() string {
	tableName := m.TableName()

	// Check cache first
	if cached := getCachedSelectBase(tableName); cached != "" {
		return cached
	}

	// Build and cache
	sb := GetStringBuilder()
	sb.WriteString("SELECT * FROM ")
	sb.WriteString(tableName)
	result := sb.String()
	PutStringBuilder(sb)

	setCachedSelectBase(tableName, result)
	return result
}

// buildInsertQuery builds an INSERT query using cached templates when possible.
// Returns the query string, columns, and placeholders.
func (m *Model[T]) buildInsertQueryCached(columns []string) string {
	tableName := m.TableName()
	columnCount := len(columns)

	// Build placeholders
	sb := GetStringBuilder()
	defer PutStringBuilder(sb)

	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableName)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(columns, ", "))
	sb.WriteString(") VALUES (")

	for i := 0; i < columnCount; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('?')
	}
	sb.WriteString(") RETURNING ")
	sb.WriteString(m.modelInfo.PrimaryKey)

	return sb.String()
}

// ClearQueryTemplateCache clears all cached query templates.
// This should be called if table schemas change at runtime (rare).
func ClearQueryTemplateCache() {
	queryTemplateCache = sync.Map{}
}
