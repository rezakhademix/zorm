package zorm

import (
	"reflect"
	"strings"
	"sync"
	"unicode"
)

// ModelInfo holds the reflection data for a model struct.
type ModelInfo struct {
	Type       reflect.Type
	TableName  string
	PrimaryKey string
	Fields     map[string]*FieldInfo // StructFieldName -> FieldInfo
	Columns    map[string]*FieldInfo // DBColumnName -> FieldInfo
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
	if typ.Kind() == reflect.Ptr {
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
		Type:    typ,
		Fields:  make(map[string]*FieldInfo),
		Columns: make(map[string]*FieldInfo),
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

	// 3. Parse Fields
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		// Skip unexported fields
		if field.PkgPath != "" {
			continue
		}

		// Skip fields with zorm:"-"
		tag := field.Tag.Get("zorm")
		if tag == "-" {
			continue
		}

		dbCol := ToSnakeCase(field.Name)
		isPrimary := false

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
				}
			}
		}

		// If field name is "ID" and no tag specified, it's primary
		if field.Name == "ID" && !isPrimary {
			isPrimary = true
		}

		// Override model primary key if found on field
		if isPrimary {
			info.PrimaryKey = dbCol
		}

		fInfo := &FieldInfo{
			Name:      field.Name,
			Column:    dbCol,
			IsPrimary: isPrimary,
			FieldType: field.Type,
			Index:     field.Index,
		}

		info.Fields[field.Name] = fInfo
		info.Columns[dbCol] = fInfo
	}

	modelCache[typ] = info
	return info
}

// ToSnakeCase converts a string to snake_case.
func ToSnakeCase(s string) string {
	var result strings.Builder
	for i, r := range s {
		if i > 0 && unicode.IsUpper(r) {
			result.WriteByte('_')
		}
		result.WriteRune(unicode.ToLower(r))
	}
	return string(result.String())
}

// fillStruct populates a struct with values from a map.
func fillStruct[T any](entity *T, data map[string]any) error {
	val := reflect.ValueOf(entity).Elem()
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		fieldVal := val.Field(i)

		// Skip unexported fields
		if !fieldVal.CanSet() {
			continue
		}

		// Find key in data (snake_case)
		key := ToSnakeCase(field.Name)
		if v, ok := data[key]; ok {
			// Simple type setting
			// TODO: robust type conversion
			safeVal := reflect.ValueOf(v)
			if safeVal.IsValid() && safeVal.Type().ConvertibleTo(fieldVal.Type()) {
				fieldVal.Set(safeVal.Convert(fieldVal.Type()))
			}
		}
	}
	return nil
}
