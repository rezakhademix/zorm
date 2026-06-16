package zorm

import (
	"container/list"
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

// sbPool is a sync.Pool for strings.Builder to reduce allocations.
// Used by query building methods.
var sbPool = sync.Pool{
	New: func() interface{} {
		return &strings.Builder{}
	},
}

// GetStringBuilder retrieves a strings.Builder from the pool.
func GetStringBuilder() *strings.Builder {
	return sbPool.Get().(*strings.Builder)
}

// PutStringBuilder returns a strings.Builder to the pool after resetting it.
func PutStringBuilder(sb *strings.Builder) {
	sb.Reset()
	sbPool.Put(sb)
}

// writePlaceholders writes n question mark placeholders separated by commas to sb.
// Example: writePlaceholders(sb, 3) writes "?,?,?"
// This avoids allocating a []string slice for strings.Join.
func writePlaceholders(sb *strings.Builder, n int) {
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('?')
	}
}

// writePlaceholdersWithSeparator writes n question mark placeholders with a custom separator.
// Example: writePlaceholdersWithSeparator(sb, 3, ", ") writes "?, ?, ?"
func writePlaceholdersWithSeparator(sb *strings.Builder, n int, sep string) {
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteString(sep)
		}
		sb.WriteByte('?')
	}
}

// snakeCaseCache caches ToSnakeCase results to avoid repeated conversions.
// Uses a bounded LRU cache to prevent unbounded memory growth if ToSnakeCase
// is called with arbitrary strings. In normal use (struct field names only),
// the cache should stabilize quickly.
var snakeCaseCache = newSnakeCaseCache(1000)

// snakeCaseCacheType is a bounded LRU cache for snake_case conversions.
// Uses sync.Mutex (not RWMutex) because Load() updates LRU order.
type snakeCaseCacheType struct {
	mu       sync.Mutex
	items    map[string]*snakeCacheEntry
	lruList  *list.List
	capacity int
}

type snakeCacheEntry struct {
	key     string
	value   string
	element *list.Element // Reference to list element for O(1) MoveToFront
}

func newSnakeCaseCache(capacity int) *snakeCaseCacheType {
	return &snakeCaseCacheType{
		items:    make(map[string]*snakeCacheEntry),
		lruList:  list.New(),
		capacity: capacity,
	}
}

func (c *snakeCaseCacheType) Load(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.items[key]; ok {
		c.lruList.MoveToFront(entry.element) // Update LRU order
		return entry.value, true
	}
	return "", false
}

func (c *snakeCaseCacheType) Store(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.items[key]; exists {
		return // Already exists
	}

	// Evict if at capacity
	if len(c.items) >= c.capacity {
		if back := c.lruList.Back(); back != nil {
			oldEntry := back.Value.(*snakeCacheEntry)
			delete(c.items, oldEntry.key)
			c.lruList.Remove(back)
		}
	}

	entry := &snakeCacheEntry{key: key, value: value}
	entry.element = c.lruList.PushFront(entry)
	c.items[key] = entry
}

// ErrInvalidColumnName is returned when a column name contains invalid characters.
var ErrInvalidColumnName = fmt.Errorf("zorm: invalid column name")

// dangerousKeywordsMap provides lookup for exact keyword matches.
// Used by ValidateColumnName to detect SQL injection attempts.
var dangerousKeywordsMap = map[string]bool{
	"union": true, "select": true, "insert": true, "update": true,
	"delete": true, "drop": true, "truncate": true, "alter": true,
	"exec": true, "execute": true, "xp_": true, "sp_": true, "0x": true,
}

// dangerousKeywordsList is used for word boundary checks (prefix/suffix with spaces).
// Package-level to avoid allocation on each ValidateColumnName call.
var dangerousKeywordsList = []string{
	"union", "select", "insert", "update", "delete", "drop", "truncate", "alter", "exec", "execute",
	"xp_", "sp_", "0x",
}

// columnValidationCache caches validation results for column names.
// Uses sync.Map for thread-safe access with good read performance.
// Value is true for valid columns (no error), false otherwise.
var columnValidationCache sync.Map

// ValidateColumnName checks if a column name is safe to use in SQL queries.
// It uses a strict whitelist approach to prevent SQL injection.
// Allowed characters: alphanumeric, underscore, dot, asterisk, space, parens, comma.
// Dangerous characters like quotes, semicolons, and comments are rejected.
// Results are cached to avoid repeated validation of the same column names.
func ValidateColumnName(name string) error {
	// Check cache first
	if valid, ok := columnValidationCache.Load(name); ok {
		if valid.(bool) {
			return nil
		}
		return fmt.Errorf("%w: '%s'", ErrInvalidColumnName, name)
	}

	err := validateColumnNameUncached(name)
	// Cache the result (true = valid, false = invalid)
	columnValidationCache.Store(name, err == nil)
	return err
}

// validateColumnNameUncached performs the actual validation without caching.
func validateColumnNameUncached(name string) error {
	if name == "" {
		return fmt.Errorf("%w: empty column name", ErrInvalidColumnName)
	}

	for _, c := range name {
		if unicode.IsLetter(c) || unicode.IsDigit(c) || c == '_' || c == '.' || c == '*' || c == ' ' || c == '(' || c == ')' || c == ',' {
			continue
		}
		// Deny everything else
		return fmt.Errorf("%w: invalid character '%c' in column name '%s'", ErrInvalidColumnName, c, name)
	}

	// Check for dangerous keywords that might be allowed by the whitelist
	// We check for whole words to avoid false positives (e.g. "update_at")
	lower := strings.ToLower(name)

	// Fast path: exact match check using map for keywords
	if dangerousKeywordsMap[lower] {
		return fmt.Errorf("%w: dangerous keyword '%s' detected in '%s'", ErrInvalidColumnName, lower, name)
	}

	// Check for word boundaries using space (simplest approach given allowed chars)
	// This catches "id UNION SELECT" but allows "union_member"
	for _, keyword := range dangerousKeywordsList {
		if strings.Contains(lower, " "+keyword+" ") ||
			strings.HasPrefix(lower, keyword+" ") ||
			strings.HasSuffix(lower, " "+keyword) {
			return fmt.Errorf("%w: dangerous keyword '%s' detected in '%s'", ErrInvalidColumnName, keyword, name)
		}
	}

	return nil
}

// MustValidateColumnName validates a column name and panics if invalid.
// Use this for internal validation where invalid column names indicate programming errors.
func MustValidateColumnName(name string) {
	if err := ValidateColumnName(name); err != nil {
		panic(err)
	}
}

// ValidateRawQuery validates a raw SQL query fragment to prevent SQL injection.
// It checks for dangerous patterns like comments, multiple statements, and suspicious keywords.
// This is used for HAVING clauses and other places where raw query fragments are accepted.
func ValidateRawQuery(query string) error {
	if query == "" {
		return fmt.Errorf("%w: empty query", ErrInvalidColumnName)
	}

	// Check for comment patterns
	if strings.Contains(query, "--") || strings.Contains(query, "/*") || strings.Contains(query, "*/") {
		return fmt.Errorf("%w: SQL comments not allowed in query '%s'", ErrInvalidColumnName, query)
	}

	// Check for multiple statements
	if strings.Contains(query, ";") {
		return fmt.Errorf("%w: multiple statements not allowed in query '%s'", ErrInvalidColumnName, query)
	}

	// Check for dangerous keywords at word boundaries
	lower := strings.ToLower(query)
	dangerousQueryKeywords := []string{"union", "insert", "update", "delete", "drop", "truncate", "alter", "exec", "execute"}
	for _, keyword := range dangerousQueryKeywords {
		if strings.Contains(lower, " "+keyword+" ") ||
			strings.HasPrefix(lower, keyword+" ") ||
			strings.HasSuffix(lower, " "+keyword) ||
			lower == keyword {
			return fmt.Errorf("%w: dangerous keyword '%s' detected in query '%s'", ErrInvalidColumnName, keyword, query)
		}
	}

	return nil
}

// ModelInfo holds the reflection data for a model struct.
type ModelInfo struct {
	Type            reflect.Type
	TableName       string
	PrimaryKey      string
	Fields          map[string]*FieldInfo // StructFieldName -> FieldInfo
	Columns         map[string]*FieldInfo // DBColumnName -> FieldInfo
	RelationFields  map[string][]int      // FieldName -> field index for FieldByIndex (relation fields)
	Accessors       []int                 // Indices of methods starting with "Get"
	RelationMethods map[string]int        // MethodName -> Index
	// VersionField, when non-nil, points to a field flagged with the `version`
	// tag modifier. Save() uses it for optimistic concurrency control:
	// the UPDATE checks the current version in WHERE and increments it.
	VersionField *FieldInfo
}

// FieldInfo holds data about a single field in the model.
// Struct layout is optimized to minimize padding on 64-bit systems.
type FieldInfo struct {
	Name      string       // 16 bytes
	Column    string       // 16 bytes
	FieldType reflect.Type // 16 bytes
	Index     []int        // 24 bytes
	IsPrimary bool         // 1 byte
	IsAuto    bool         // 1 byte + 6 padding
}

// GetRelationField returns the reflect.Value for a relation field by name.
// Uses cached field indices for O(1) access instead of O(n) FieldByName.
// Returns invalid Value if field not found.
func (m *ModelInfo) GetRelationField(structVal reflect.Value, fieldName string) reflect.Value {
	if idx, ok := m.RelationFields[fieldName]; ok {
		return structVal.FieldByIndex(idx)
	}
	// Fallback to FieldByName for backwards compatibility
	return structVal.FieldByName(fieldName)
}

// modelCache stores parsed ModelInfo for each type.
// Uses sync.Map for optimal read-heavy workloads with infrequent writes,
// which matches the ORM's usage pattern (parse once, read many times).
var modelCache sync.Map

// ParseModel inspects the struct T and returns its metadata.
func ParseModel[T any]() *ModelInfo {
	var t T
	typ := reflect.TypeOf(t)
	return ParseModelType(typ)
}

// ParseModelType inspects the type and returns its metadata.
// Uses sync.Map for thread-safe caching with optimal read performance.
func ParseModelType(typ reflect.Type) *ModelInfo {
	// Handle pointer types
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		// This might happen if T is not a struct, but we expect models to be structs.
		// For now, we'll panic or return nil, but panic is safer for dev feedback.
		panic("ZORM: Model generic type T must be a struct")
	}

	// Fast path: check if already cached
	if cached, ok := modelCache.Load(typ); ok {
		return cached.(*ModelInfo)
	}

	// Slow path: parse the model
	// Note: Multiple goroutines may parse the same type concurrently on first access.
	// This is acceptable as parsing is idempotent and LoadOrStore ensures only one wins.
	info := &ModelInfo{
		Type:            typ,
		Fields:          make(map[string]*FieldInfo),
		Columns:         make(map[string]*FieldInfo),
		RelationFields:  make(map[string][]int),
		RelationMethods: make(map[string]int),
	}

	// 1. Determine Table Name
	// Check if T implements TableName() string
	// We need a pointer to T to call methods if the receiver is a pointer
	ptrVal := reflect.New(typ)
	if tableNameer, ok := ptrVal.Interface().(interface{ TableName() string }); ok {
		info.TableName = tableNameer.TableName()
	} else {
		info.TableName = ToSnakeCase(typ.Name()) + "s" // Simple pluralization
	}

	// 2. Determine Primary Key
	// Check if T implements PrimaryKey() string
	if primaryKeyer, ok := ptrVal.Interface().(interface{ PrimaryKey() string }); ok {
		info.PrimaryKey = primaryKeyer.PrimaryKey()
	} else {
		info.PrimaryKey = "id" // Default
	}

	// 3. Parse Fields (including embedded)
	parseFields(typ, info, []int{})

	// 4. Parse Accessors (Get methods) and Relation Methods
	// We store valid methods for quick access during scanning and relation loading
	relationType := reflect.TypeOf((*Relation)(nil)).Elem()

	for i := 0; i < typ.NumMethod(); i++ {
		method := typ.Method(i)

		// Accessor convention: Starts with "Get", has 0 arguments, returns 1 value
		if strings.HasPrefix(method.Name, "Get") && method.Type.NumIn() == 1 && method.Type.NumOut() == 1 {
			info.Accessors = append(info.Accessors, i)
		}

		// Relation Method detection
		// Must return 1 value that implements Relation interface
		if method.Type.NumIn() == 1 && method.Type.NumOut() == 1 {
			if method.Type.Out(0).Implements(relationType) {
				info.RelationMethods[method.Name] = i
			}
		}
	}

	// Store in cache; if another goroutine stored first, use their value
	actual, _ := modelCache.LoadOrStore(typ, info)
	return actual.(*ModelInfo)
}

func parseFields(typ reflect.Type, info *ModelInfo, indexPrefix []int) {
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		// Skip unexported fields
		if field.PkgPath != "" {
			continue
		}

		// Handle Embedded Structs
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			newIndex := append(indexPrefix, i)
			parseFields(field.Type, info, newIndex)
			continue
		}

		// Record relation fields for efficient FieldByIndex lookups during relation loading
		if isRelationField(field.Type) {
			currentIndex := append(indexPrefix, i)
			finalIndex := make([]int, len(currentIndex))
			copy(finalIndex, currentIndex)
			info.RelationFields[field.Name] = finalIndex
			continue
		}

		// Skip fields with zorm:"-"
		tag := field.Tag.Get("zorm")
		if tag == "-" {
			continue
		}

		dbCol := ToSnakeCase(field.Name)
		isPrimary := false
		isAuto := false
		isVersion := false

		// Parse tag
		if tag != "" {
			parts := strings.Split(tag, ";")
			for _, part := range parts {
				kv := strings.Split(part, ":")
				key := strings.TrimSpace(kv[0])
				val := ""
				if len(kv) > 1 {
					val = strings.TrimSpace(kv[1])
				}

				switch key {
				case "column":
					dbCol = val
				case "primary":
					isPrimary = true
				case "auto":
					isAuto = true
				case "primaryKey":
					isPrimary = true
					if isIntegerKind(field.Type.Kind()) {
						isAuto = true
					}
				case "version":
					isVersion = true
				default:
					// Bare-form shorthand: `zorm:"full_name"` overrides the
					// column name. Only fires when the token has no `:` and
					// the value is a valid bare column identifier (no commas,
					// spaces, dots, parens, etc.) so common typos like
					// `zorm:"primary,auto"` panic loudly instead of silently
					// renaming the column to garbage.
					if key != "" && !strings.Contains(part, ":") {
						if err := validateBareColumnTag(key); err != nil {
							panic(fmt.Sprintf("zorm: field %q on %s: invalid bare-form tag %q: %v (use `column:%s` for explicit column override, or `;` to separate multiple tag tokens)",
								field.Name, typ.Name(), key, err, key))
						}
						dbCol = key
					}
				}
			}
		}

		// If field name is "ID" and no tag specified, it's primary
		if field.Name == "ID" && !isPrimary {
			isPrimary = true
			isAuto = true // Default ID to auto-increment
		}

		// Override model primary key if found on field
		if isPrimary {
			info.PrimaryKey = dbCol
		}

		// Construct full index path
		currentIndex := append(indexPrefix, i)
		// We need to copy the slice to avoid sharing backing array issues in recursion?
		// append creates a new slice if capacity is exceeded, but better be safe.
		// Actually, since we pass by value (slice header), and append returns new header,
		// it should be fine as long as we don't modify the passed slice.
		// But `indexPrefix` is reused in the loop? No, it's constant for this call.
		// `newIndex` in recursive call is a new slice.
		// `currentIndex` here is a new slice.
		// However, `indexPrefix` backing array might be shared.
		// To be safe, let's force copy if needed, but `append` usually handles it.
		// Let's explicitly copy to be 100% safe against subtle bugs.
		finalIndex := make([]int, len(currentIndex))
		copy(finalIndex, currentIndex)

		fInfo := &FieldInfo{
			Name:      field.Name,
			Column:    dbCol,
			IsPrimary: isPrimary,
			IsAuto:    isAuto,
			FieldType: field.Type,
			Index:     finalIndex,
		}

		info.Fields[field.Name] = fInfo
		info.Columns[dbCol] = fInfo

		if isVersion {
			if !isVersionableKind(field.Type.Kind()) {
				panic(fmt.Sprintf("zorm: field %q on %s tagged `version` must be an integer type, got %s",
					field.Name, typ.Name(), field.Type.Kind()))
			}
			if info.VersionField != nil {
				panic(fmt.Sprintf("zorm: model %s declares more than one `version` field (%s and %s)",
					typ.Name(), info.VersionField.Name, field.Name))
			}
			info.VersionField = fInfo
		}
	}
}

// isIntegerKind reports whether the kind is a signed or unsigned integer.
// Used by the `primaryKey` shorthand to decide whether to imply auto-increment.
func isIntegerKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	}
	return false
}

// validateBareColumnTag enforces that a bare-form `zorm:"name"` value is a
// plain SQL identifier: ASCII letters/digits/underscore, starting with a
// letter or underscore. Rejects commas, spaces, dots, parens, and other
// characters that ValidateColumnName permits in SELECT lists but that are
// meaningless as a column-rename target. Catches typos like
// `zorm:"primary,auto"` (which should have been semicolon-separated).
func validateBareColumnTag(s string) error {
	if s == "" {
		return fmt.Errorf("empty column name")
	}
	for i, r := range s {
		ok := r == '_' ||
			(r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(i > 0 && r >= '0' && r <= '9')
		if !ok {
			return fmt.Errorf("invalid character %q at position %d", r, i)
		}
	}
	return nil
}

// versionKindTable is the single source of truth for which reflect.Kind
// values are accepted as optimistic-lock `version` columns and what their
// safe upper bound is. Adding a new supported kind here automatically wires
// it through isVersionableKind, maxVersionForKind, toInt64Version, and
// setInt64Version — eliminating the four-parallel-switch hazard the
// previous implementation had.
var versionKindTable = map[reflect.Kind]int64{
	reflect.Int:    math.MaxInt64,
	reflect.Int32:  math.MaxInt32,
	reflect.Int64:  math.MaxInt64,
	reflect.Uint:   math.MaxInt64, // toInt64Version casts uint→int64
	reflect.Uint32: math.MaxUint32,
	reflect.Uint64: math.MaxInt64, // values above MaxInt64 already broken upstream
}

// isVersionableKind reports whether kind is a supported integer kind for an
// optimistic-lock version column.
func isVersionableKind(k reflect.Kind) bool {
	_, ok := versionKindTable[k]
	return ok
}

// maxVersionForKind returns the largest int64 value that can be safely
// stored in the given reflect.Kind without overflow when written back via
// setInt64Version. Returns math.MaxInt64 for unsupported kinds (defensive).
func maxVersionForKind(k reflect.Kind) int64 {
	if max, ok := versionKindTable[k]; ok {
		return max
	}
	return math.MaxInt64
}

// toInt64Version reads an int* / uint* reflect.Value as int64. Caller
// guarantees the kind is one accepted by isVersionableKind.
func toInt64Version(v reflect.Value) int64 {
	switch v.Kind() {
	case reflect.Int, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Uint, reflect.Uint32, reflect.Uint64:
		return int64(v.Uint())
	}
	return 0
}

// setInt64Version writes n into an int* / uint* reflect.Value, mirroring the
// kinds accepted by toInt64Version.
func setInt64Version(v reflect.Value, n int64) {
	if !v.CanSet() {
		return
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int32, reflect.Int64:
		v.SetInt(n)
	case reflect.Uint, reflect.Uint32, reflect.Uint64:
		v.SetUint(uint64(n))
	}
}

// timeType is cached to avoid repeated reflect.TypeOf calls.
var timeType = reflect.TypeOf(time.Time{})

// isRelationField reports whether t represents a relation field that should be
// excluded from database queries. Relation fields are:
//   - Pointers to structs (e.g., *Branch) except *time.Time
//   - Slices of structs or pointers to structs (e.g., []*Post, []Post)
func isRelationField(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Ptr:
		return isRelationStruct(t.Elem())
	case reflect.Slice:
		elem := t.Elem()
		if elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}
		return isRelationStruct(elem)
	default:
		return false
	}
}

// isRelationStruct reports whether t is a struct type representing a relation.
// time.Time is excluded as it's a valid database column type.
func isRelationStruct(t reflect.Type) bool {
	return t.Kind() == reflect.Struct && t != timeType
}

// ToSnakeCase converts a string to snake_case.
// Handles acronyms correctly (e.g., UserID -> user_id, HTTPClient -> http_client).
// Results are cached to avoid repeated conversions for the same input.
func ToSnakeCase(s string) string {
	// Check cache first
	if cached, ok := snakeCaseCache.Load(s); ok {
		return cached
	}

	sb := GetStringBuilder()
	defer PutStringBuilder(sb)
	sb.Grow(len(s) + 5) // Pre-allocate some space

	for i, r := range s {
		if i > 0 {
			// If current is upper
			if unicode.IsUpper(r) {
				// Check previous char
				prev := rune(s[i-1])
				// If previous was lower, we definitely need underscore (e.g. aB -> a_b)
				if unicode.IsLower(prev) {
					sb.WriteByte('_')
				} else if unicode.IsUpper(prev) {
					// If previous was upper, we might need underscore if next is lower
					// e.g. HTTPClient -> HTTP_Client (at 'C')
					// We want http_client.
					// So if current is upper, and next is lower, and previous is upper -> underscore before current.
					if i+1 < len(s) {
						next := rune(s[i+1])
						if unicode.IsLower(next) {
							sb.WriteByte('_')
						}
					}
				} else if unicode.IsDigit(prev) {
					// 1A -> 1_a
					sb.WriteByte('_')
				}
			} else if unicode.IsDigit(r) {
				// a1 -> a_1
				prev := rune(s[i-1])
				if !unicode.IsDigit(prev) {
					sb.WriteByte('_')
				}
			}
		}
		sb.WriteRune(unicode.ToLower(r))
	}

	converted := sb.String()
	snakeCaseCache.Store(s, converted)
	return converted
}

// fillStruct populates a struct with values from a map using ModelInfo.
func fillStruct[T any](entity *T, data map[string]any) error {
	val := reflect.ValueOf(entity).Elem()
	// We need ModelInfo to know mapping
	info := ParseModel[T]()

	for colName, valData := range data {
		// Find field info by column name
		fieldInfo, ok := info.Columns[colName]
		if !ok {
			continue
		}

		// Get field value using Index (supports embedded structs)
		fieldVal := val.FieldByIndex(fieldInfo.Index)

		if !fieldVal.CanSet() {
			continue
		}

		// Set value with conversion
		if err := setFieldValue(fieldVal, valData); err != nil {
			return fmt.Errorf("failed to set field %s (col %s): %w", fieldInfo.Name, colName, err)
		}
	}
	return nil
}

// setFieldValue sets a reflect.Value with type conversion.
func setFieldValue(field reflect.Value, value any) error {
	if value == nil {
		// If field is a pointer, set to nil
		if field.Kind() == reflect.Pointer {
			field.Set(reflect.Zero(field.Type()))
			return nil
		}
		// If field is not pointer, we can't set nil. Ignore or error?
		// Ignore is safer for partial updates.
		return nil
	}

	// Handle sql.Scanner
	if scanner, ok := field.Addr().Interface().(sql.Scanner); ok {
		return scanner.Scan(value)
	}

	// Handle pointer fields: we want to set the element
	if field.Kind() == reflect.Pointer {
		// If value is nil, handled above.
		// Allocate new value if nil
		if field.IsNil() {
			field.Set(reflect.New(field.Type().Elem()))
		}
		// Recurse to set the element
		return setFieldValue(field.Elem(), value)
	}

	val := reflect.ValueOf(value)
	valType := val.Type()
	fieldType := field.Type()

	// Direct assignment if types match
	if valType.AssignableTo(fieldType) {
		field.Set(val)
		return nil
	}

	// Type Conversion
	if valType.ConvertibleTo(fieldType) {
		field.Set(val.Convert(fieldType))
		return nil
	}

	// Common conversions (e.g. int64 -> int, []byte -> string)
	switch field.Kind() {
	case reflect.String:
		if val.Kind() == reflect.Slice && valType.Elem().Kind() == reflect.Uint8 {
			// []byte to string
			field.SetString(string(val.Bytes()))
			return nil
		}
		// Fallback to fmt.Sprint
		field.SetString(fmt.Sprint(value))
		return nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return setIntField(field, value)

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return setUintField(field, value)

	case reflect.Float32, reflect.Float64:
		return setFloatField(field, value)

	case reflect.Bool:
		return setBoolField(field, value)
	}

	return fmt.Errorf("unsupported type conversion from %T to %s", value, fieldType)
}

func setIntField(field reflect.Value, value any) error {
	switch v := value.(type) {
	case int64:
		field.SetInt(v)
	case int:
		field.SetInt(int64(v))
	case int32:
		field.SetInt(int64(v))
	case int16:
		field.SetInt(int64(v))
	case int8:
		field.SetInt(int64(v))
	case float64:
		field.SetInt(int64(v))
	case float32:
		field.SetInt(int64(v))
	case []byte:
		i, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return err
		}
		field.SetInt(i)
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return err
		}
		field.SetInt(i)
	default:
		return fmt.Errorf("cannot convert %T to int", value)
	}
	return nil
}

func setUintField(field reflect.Value, value any) error {
	switch v := value.(type) {
	case int64:
		field.SetUint(uint64(v))
	case uint64:
		field.SetUint(v)
	case int:
		field.SetUint(uint64(v))
	case float64:
		field.SetUint(uint64(v))
	case []byte:
		i, err := strconv.ParseUint(string(v), 10, 64)
		if err != nil {
			return err
		}
		field.SetUint(i)
	case string:
		i, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return err
		}
		field.SetUint(i)
	default:
		return fmt.Errorf("cannot convert %T to uint", value)
	}
	return nil
}

func setFloatField(field reflect.Value, value any) error {
	switch v := value.(type) {
	case float64:
		field.SetFloat(v)
	case float32:
		field.SetFloat(float64(v))
	case int64:
		field.SetFloat(float64(v))
	case int:
		field.SetFloat(float64(v))
	case []byte:
		f, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return err
		}
		field.SetFloat(f)
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return err
		}
		field.SetFloat(f)
	default:
		return fmt.Errorf("cannot convert %T to float", value)
	}
	return nil
}

func setBoolField(field reflect.Value, value any) error {
	switch v := value.(type) {
	case bool:
		field.SetBool(v)
	case int64:
		field.SetBool(v != 0)
	case int:
		field.SetBool(v != 0)
	case string:
		b, err := strconv.ParseBool(v)
		if err != nil {
			return err
		}
		field.SetBool(b)
	case []byte:
		b, err := strconv.ParseBool(string(v))
		if err != nil {
			return err
		}
		field.SetBool(b)
	default:
		return fmt.Errorf("cannot convert %T to bool", value)
	}
	return nil
}
