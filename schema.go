package zorm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
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

// snakeCaseCache caches ToSnakeCase results to avoid repeated conversions.
var snakeCaseCache sync.Map

// ErrInvalidColumnName is returned when a column name contains invalid characters.
var ErrInvalidColumnName = fmt.Errorf("zorm: invalid column name")

// ValidateColumnName checks if a column name is safe to use in SQL queries.
// It uses a strict whitelist approach to prevent SQL injection.
// Allowed characters: alphanumeric, underscore, dot, asterisk, space, parens, comma.
// Dangerous characters like quotes, semicolons, and comments are rejected.
func ValidateColumnName(name string) error {
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
	dangerousKeywords := []string{
		"union", "select", "insert", "update", "delete", "drop", "truncate", "alter", "exec", "execute",
		"xp_", "sp_", "0x",
	}

	for _, keyword := range dangerousKeywords {
		// Check for word boundaries using space (simplest approach given allowed chars)
		// This catches "id UNION SELECT" but allows "union_member"
		if strings.Contains(lower, " "+keyword+" ") ||
			strings.HasPrefix(lower, keyword+" ") ||
			strings.HasSuffix(lower, " "+keyword) ||
			lower == keyword {
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

// ModelInfo holds the reflection data for a model struct.
type ModelInfo struct {
	Type            reflect.Type
	TableName       string
	PrimaryKey      string
	Fields          map[string]*FieldInfo // StructFieldName -> FieldInfo
	Columns         map[string]*FieldInfo // DBColumnName -> FieldInfo
	Accessors       []int                 // Indices of methods starting with "Get"
	RelationMethods map[string]int        // MethodName -> Index
}

// FieldInfo holds data about a single field in the model.
type FieldInfo struct {
	Name      string // Struct field name
	Column    string // DB column name
	IsPrimary bool
	IsAuto    bool // Auto-increment or managed
	FieldType reflect.Type
	Index     []int // Index path for nested fields (if we support embedding)
}

var (
	modelCache = make(map[reflect.Type]*ModelInfo)
	cacheMu    sync.RWMutex
)

// ParseModel inspects the struct T and returns its metadata.
func ParseModel[T any]() *ModelInfo {
	var t T
	typ := reflect.TypeOf(t)
	return ParseModelType(typ)
}

// ParseModelType inspects the type and returns its metadata.
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

	cacheMu.RLock()
	if info, ok := modelCache[typ]; ok {
		cacheMu.RUnlock()
		return info
	}
	cacheMu.RUnlock()

	cacheMu.Lock()
	defer cacheMu.Unlock()

	// Double check locking
	if info, ok := modelCache[typ]; ok {
		return info
	}

	info := &ModelInfo{
		Type:            typ,
		Fields:          make(map[string]*FieldInfo),
		Columns:         make(map[string]*FieldInfo),
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

	modelCache[typ] = info
	return info
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

		// Skip fields with zorm:"-"
		tag := field.Tag.Get("zorm")
		if tag == "-" {
			continue
		}

		dbCol := ToSnakeCase(field.Name)
		isPrimary := false
		isAuto := false

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
	}
}

// ToSnakeCase converts a string to snake_case.
// Handles acronyms correctly (e.g., UserID -> user_id, HTTPClient -> http_client).
// Results are cached to avoid repeated conversions for the same input.
func ToSnakeCase(s string) string {
	// Check cache first
	if cached, ok := snakeCaseCache.Load(s); ok {
		return cached.(string)
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
