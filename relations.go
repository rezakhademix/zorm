package zorm

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// anyToKeyString converts common types to string keys efficiently.
// Falls back to fmt.Sprintf for uncommon types.
func anyToKeyString(v any) string {
	switch x := v.(type) {
	case int64:
		return strconv.FormatInt(x, 10)
	case int:
		return strconv.Itoa(x)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int16:
		return strconv.FormatInt(int64(x), 10)
	case int8:
		return strconv.FormatInt(int64(x), 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case uint:
		return strconv.FormatUint(uint64(x), 10)
	case uint32:
		return strconv.FormatUint(uint64(x), 10)
	case uint16:
		return strconv.FormatUint(uint64(x), 10)
	case uint8:
		return strconv.FormatUint(uint64(x), 10)
	case string:
		return x
	case []byte:
		// Common for UUIDs stored as binary
		return string(x)
	case [16]byte:
		// UUID as fixed-size array (common representation)
		return string(x[:])
	default:
		// Check for fmt.Stringer interface (covers uuid.UUID and similar types)
		if s, ok := v.(fmt.Stringer); ok {
			return s.String()
		}
		return fmt.Sprintf("%v", v)
	}
}

// RelationType defines the type of relationship between two models in the ORM.
type RelationType string

const (
	// RelationHasOne represents a one-to-one relationship where the current
	// model owns a single related record.
	RelationHasOne RelationType = "HasOne"

	// RelationHasMany represents a one-to-many relationship where the current
	// model owns multiple related records.
	RelationHasMany RelationType = "HasMany"

	// RelationBelongsTo represents an inverse one-to-one or one-to-many
	// relationship where the current model references a parent record.
	RelationBelongsTo RelationType = "BelongsTo"

	// RelationBelongsToMany represents a many-to-many relationship between
	// two models, typically connected through a join table.
	RelationBelongsToMany RelationType = "BelongsToMany"
)

// RelationDefinition holds metadata about a relation.
type RelationDefinition struct {
	Type        RelationType
	Field       string // The struct field name in the parent model
	RelatedType reflect.Type

	// Keys
	ForeignKey string
	LocalKey   string
	OwnerKey   string // For BelongsTo

	// Pivot (BelongsToMany)
	PivotTable   string
	PivotForeign string
	PivotRelated string
}

// HasOne defines a HasOne relation.
type HasOne[T any] struct {
	ForeignKey string
	LocalKey   string
	Table      string
}

// HasMany defines a HasMany relation.
type HasMany[T any] struct {
	ForeignKey string
	LocalKey   string
	Table      string
}

// BelongsTo defines a BelongsTo relation.
type BelongsTo[T any] struct {
	ForeignKey string
	OwnerKey   string
	Table      string
}

// BelongsToMany defines a BelongsToMany relation.
type BelongsToMany[T any] struct {
	PivotTable string
	ForeignKey string
	RelatedKey string
	LocalKey   string
	RelatedPK  string
	Table      string
}

// MorphTo defines a polymorphic BelongsTo relation.
// T is usually `any` or a common interface, but in our generic system,
// the field in the struct will likely be `any` or an interface.
//
// Unlike other relation types, MorphTo cannot implement NewRelated() and NewModel()
// in a meaningful way because the related type is determined dynamically at runtime
// based on the Type column value. These methods intentionally return nil.
// The TypeMap field maps database type strings to empty struct instances, which are
// used during eager loading to determine and instantiate the correct related type.
//
// TypeMap keys are the values stored in the type column, which is what the
// parent model reports as its morph type: its MorphType() string method when it
// declares one, otherwise its Go struct name. Use the same values that
// MorphOne/MorphMany match on, or the two directions of the same relation will
// disagree.
//
// Example:
//
//	// Post declares its morph type, so its rows are tagged "posts".
//	func (p Post) MorphType() string { return "posts" }
//
//	func (i *Image) ImageableRelation() MorphTo[any] {
//	    return MorphTo[any]{
//	        Type: "imageable_type",
//	        ID:   "imageable_id",
//	        TypeMap: map[string]any{
//	            "posts": Post{},
//	            "User":  User{}, // User has no MorphType(): struct name
//	        },
//	    }
//	}
type MorphTo[T any] struct {
	// Type is the DB column holding the related type (e.g. imageable_type).
	// A struct field name is also accepted for backward compatibility.
	Type string
	// ID is the DB column holding the related id (e.g. imageable_id).
	// A struct field name is also accepted for backward compatibility.
	ID      string
	TypeMap map[string]any // Map of DB type string to empty struct instance (e.g. "posts": Post{})
}

// MorphOne defines a polymorphic HasOne relation.
//
// The loader matches rows whose Type column equals the parent model's morph
// type: its MorphType() string method when declared, otherwise the Go struct
// name.
type MorphOne[T any] struct {
	Type  string // Column name in related table (e.g. imageable_type)
	ID    string // Column name in related table (e.g. imageable_id)
	Table string
}

// MorphMany defines a polymorphic HasMany relation.
//
// The loader matches rows whose Type column equals the parent model's morph
// type: its MorphType() string method when declared, otherwise the Go struct
// name.
type MorphMany[T any] struct {
	Type  string // Column name in related table (e.g. imageable_type)
	ID    string // Column name in related table (e.g. imageable_id)
	Table string
}

// Relation interface allows us to handle generics uniformly.
type Relation interface {
	RelationType() RelationType
	NewRelated() any
	NewModel(ctx context.Context, db *sql.DB) any
}

func (HasOne[T]) RelationType() RelationType { return RelationHasOne }
func (HasOne[T]) NewRelated() any            { return new(T) }
func (HasOne[T]) NewModel(ctx context.Context, db *sql.DB) any {
	m := New[T]()
	m.db = db
	m.ctx = ctx
	return m
}

func (HasMany[T]) RelationType() RelationType { return RelationHasMany }
func (HasMany[T]) NewRelated() any            { return new(T) }
func (HasMany[T]) NewModel(ctx context.Context, db *sql.DB) any {
	m := New[T]()
	m.db = db
	m.ctx = ctx
	return m
}

func (BelongsTo[T]) RelationType() RelationType { return RelationBelongsTo }
func (BelongsTo[T]) NewRelated() any            { return new(T) }
func (BelongsTo[T]) NewModel(ctx context.Context, db *sql.DB) any {
	m := New[T]()
	m.db = db
	m.ctx = ctx
	return m
}

func (BelongsToMany[T]) RelationType() RelationType { return RelationBelongsToMany }
func (BelongsToMany[T]) NewRelated() any            { return new(T) }
func (BelongsToMany[T]) NewModel(ctx context.Context, db *sql.DB) any {
	m := New[T]()
	m.db = db
	m.ctx = ctx
	return m
}

const (
	// RelationMorphTo represents a polymorphic inverse relationship where the
	// current model can belong to one of several different model types. The
	// actual target type and ID are determined by discriminator columns such as
	// "morph_type" and "morph_id".
	RelationMorphTo RelationType = "MorphTo"

	// RelationMorphOne represents a polymorphic one-to-one relationship where a
	// single related record can be associated with multiple possible parent
	// model types.
	RelationMorphOne RelationType = "MorphOne"

	// RelationMorphMany represents a polymorphic one-to-many relationship where
	// multiple related records can be associated with various parent model types.
	RelationMorphMany RelationType = "MorphMany"
)

// MorphTo implements Relation interface.
// RelationType returns RelationMorphTo.
func (MorphTo[T]) RelationType() RelationType { return RelationMorphTo }

// NewRelated returns nil for MorphTo because the related type is determined
// dynamically at runtime based on the type column value. Use TypeMap instead.
func (MorphTo[T]) NewRelated() any { return nil }

// NewModel returns nil for MorphTo because the related model type is determined
// dynamically at runtime based on the type column value.
func (MorphTo[T]) NewModel(ctx context.Context, db *sql.DB) any { return nil }

func (MorphOne[T]) RelationType() RelationType { return RelationMorphOne }
func (MorphOne[T]) NewRelated() any            { return new(T) }
func (MorphOne[T]) NewModel(ctx context.Context, db *sql.DB) any {
	m := New[T]()
	m.db = db
	m.ctx = ctx
	return m
}

func (MorphMany[T]) RelationType() RelationType { return RelationMorphMany }
func (MorphMany[T]) NewRelated() any            { return new(T) }
func (MorphMany[T]) NewModel(ctx context.Context, db *sql.DB) any {
	m := New[T]()
	m.db = db
	m.ctx = ctx
	return m
}

// TableOverrider interface allows relations to specify a custom table name.
type TableOverrider interface {
	GetOverrideTable() string
}

func (r HasOne[T]) GetOverrideTable() string    { return r.Table }
func (r HasMany[T]) GetOverrideTable() string   { return r.Table }
func (r BelongsTo[T]) GetOverrideTable() string { return r.Table }
func (r MorphOne[T]) GetOverrideTable() string  { return r.Table }
func (r MorphMany[T]) GetOverrideTable() string { return r.Table }

// Load eager loads relations on a single entity.
// This method creates an internal clone to avoid mutating the original model's state,
// making it safe to reuse the model for subsequent queries.
func (m *Model[T]) Load(ctx context.Context, entity *T, relations ...string) error {
	// Clone to avoid mutating shared state
	q := m.Clone()
	q.relations = append(q.relations, relations...)
	return q.loadRelations(ctx, []*T{entity})
}

// LoadSlice eager loads relations on a slice of entities.
// This method creates an internal clone to avoid mutating the original model's state,
// making it safe to reuse the model for subsequent queries.
func (m *Model[T]) LoadSlice(ctx context.Context, entities []*T, relations ...string) error {
	// Clone to avoid mutating shared state
	q := m.Clone()
	q.relations = append(q.relations, relations...)
	return q.loadRelations(ctx, entities)
}

// LoadMorph eager loads a polymorphic relation with constraints on a slice.
// This method creates an internal clone to avoid mutating the original model's state,
// making it safe to reuse the model for subsequent queries.
func (m *Model[T]) LoadMorph(ctx context.Context, entities []*T, relation string, typeMap map[string][]string) error {
	// Clone to avoid mutating shared state
	q := m.Clone()
	if q.morphRelations == nil {
		q.morphRelations = make(map[string]map[string][]string)
	}
	q.relations = append(q.relations, relation)
	q.morphRelations[relation] = typeMap
	return q.loadRelations(ctx, entities)
}

// loadRelations processes the With() clauses and loads data.
func (m *Model[T]) loadRelations(ctx context.Context, results []*T) error {
	if len(m.relations) == 0 || len(results) == 0 {
		return nil
	}

	// Group relations by root
	// Map: RootRelation -> {Cols, []SubRelations}
	type relGroup struct {
		Cols string
		Subs []string
	}
	groups := make(map[string]*relGroup)

	for _, relationName := range m.relations {
		// Handle "relation:cols" syntax
		parts := strings.Split(relationName, ":")
		path := parts[0]
		cols := ""
		if len(parts) > 1 {
			cols = parts[1]
		}

		// Handle dot notation "A.B"
		dotParts := strings.SplitN(path, ".", 2)
		root := dotParts[0]

		if _, ok := groups[root]; !ok {
			groups[root] = &relGroup{}
		}

		if len(dotParts) > 1 {
			// It's a nested relation, add to Subs
			// We need to preserve the cols for the leaf?
			// Actually, "A.B:cols" means cols apply to B.
			// "A:cols.B" is invalid syntax usually.
			// Laravel: "A:id,name", "A.B"
			// If we have "A.B:cols", we pass "B:cols" to A's loader.
			sub := dotParts[1]
			if cols != "" {
				sub += ":" + cols
			}
			groups[root].Subs = append(groups[root].Subs, sub)
		} else {
			// It's the root itself.
			if cols != "" {
				groups[root].Cols = cols
			}
		}
	}

	for relName, group := range groups {
		// Find the method on T using cached index
		var t T
		var methodVal reflect.Value

		if idx, ok := m.modelInfo.RelationMethods[relName]; ok {
			methodVal = reflect.ValueOf(t).Method(idx)
		} else if idx, ok := m.modelInfo.RelationMethods[relName+"Relation"]; ok {
			methodVal = reflect.ValueOf(t).Method(idx)
		} else {
			return WrapRelationError(relName, fmt.Sprintf("%T", t), ErrRelationNotFound)
		}

		// Call the method to get the relation config
		retVals := methodVal.Call(nil)
		if len(retVals) == 0 {
			return fmt.Errorf("relation method %s must return a value", relName)
		}

		relConfig := retVals[0].Interface()

		// Extract callback constraints if WithCallback was used for this relation
		var constraints *relationConstraints
		if callback, ok := m.relationCallbacks[relName]; ok {
			if rel, ok := relConfig.(Relation); ok {
				db := m.db
				if db == nil {
					db = GetGlobalDB()
				}
				constraints = extractRelationConstraints(rel, callback, ctx, db)
				if constraints != nil && constraints.buildErr != nil {
					return constraints.buildErr
				}
			}
		}

		// Dispatch based on type
		if rel, ok := relConfig.(Relation); ok {
			switch rel.RelationType() {
			case RelationHasMany:
				// Pass group.Subs to loadHasMany to handle recursion
				if err := m.loadHasMany(ctx, results, relConfig, relName, group.Cols, group.Subs, constraints); err != nil {
					return err
				}
			case RelationHasOne:
				// HasOne uses HasMany logic but assigns single value
				if err := m.loadHasMany(ctx, results, relConfig, relName, group.Cols, group.Subs, constraints); err != nil {
					return err
				}
			case RelationBelongsTo:
				if err := m.loadBelongsTo(ctx, results, relConfig, relName, group.Cols, group.Subs, constraints); err != nil {
					return err
				}
			case RelationMorphTo:
				// Check for morph constraints
				var subMap map[string][]string
				if m.morphRelations != nil {
					subMap = m.morphRelations[relName]
				}
				if err := m.loadMorphTo(ctx, results, relConfig, relName, subMap); err != nil {
					return err
				}
			case RelationMorphOne:
				if err := m.loadMorphOneOrMany(ctx, results, relConfig, relName, group.Cols, group.Subs, constraints); err != nil {
					return err
				}
			case RelationMorphMany:
				if err := m.loadMorphOneOrMany(ctx, results, relConfig, relName, group.Cols, group.Subs, constraints); err != nil {
					return err
				}
			case RelationBelongsToMany:
				if err := m.loadBelongsToMany(ctx, results, relConfig, relName, group.Cols, group.Subs, constraints); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// morphField resolves a MorphTo Type/ID identifier against a parent struct
// value. The documented form is the DB column name (the same form MorphOne and
// MorphMany take); a struct field name is accepted as a fallback so configs
// written against the older behavior keep working.
func (m *Model[T]) morphField(val reflect.Value, name string) reflect.Value {
	if field, ok := m.modelInfo.Columns[name]; ok {
		return val.FieldByIndex(field.Index)
	}
	return val.FieldByName(name)
}

func (m *Model[T]) loadMorphTo(ctx context.Context, results []*T, relConfig any, relName string, typeMap map[string][]string) error {
	// 1. Get Type and ID fields from MorphTo config
	morphRel, ok := relConfig.(MorphTo[any])
	if !ok {
		return fmt.Errorf("relation %s: expected MorphTo[any], got %T", relName, relConfig)
	}
	// Type and ID are DB column names, matching MorphOne/MorphMany. A struct
	// field name is still accepted for configs written against the older
	// behavior; morphField resolves either form to the same field.
	typeField := morphRel.Type
	idField := morphRel.ID

	// 2. Group IDs by Type
	// Map: Type -> []ID
	idsByType := make(map[string][]any)
	// Map: Type -> normalized ID key -> []ParentIndices. The key is normalized
	// with anyToKeyString like every other relation loader, so a parent key and
	// a child key of different integer widths still match and a []byte key does
	// not panic.
	parentMap := make(map[string]map[string][]int)

	for i, res := range results {
		val := reflect.ValueOf(res).Elem()

		tf := m.morphField(val, typeField)
		if !tf.IsValid() {
			continue
		}

		var typeValue string
		if tf.Kind() == reflect.Ptr {
			if tf.IsNil() {
				continue
			}
			typeValue = tf.Elem().String()
		} else {
			typeValue = tf.String()
		}

		if typeValue == "" {
			continue
		}

		// Get ID
		idf := m.morphField(val, idField)
		if !idf.IsValid() {
			continue
		}

		var idValue any
		if idf.Kind() == reflect.Ptr {
			if idf.IsNil() {
				continue
			}
			idValue = idf.Elem().Interface()
		} else {
			idValue = idf.Interface()
		}

		idsByType[typeValue] = append(idsByType[typeValue], idValue)

		if _, ok := parentMap[typeValue]; !ok {
			parentMap[typeValue] = make(map[string][]int)
		}
		idKey := anyToKeyString(idValue)
		parentMap[typeValue][idKey] = append(parentMap[typeValue][idKey], i)
	}

	// 3. Query each type
	for typeName, ids := range idsByType {
		// Find Model for TypeName
		// We use morphRel.TypeMap
		modelInstance, ok := morphRel.TypeMap[typeName]
		if !ok {
			// Type not found in map, skip
			continue
		}

		// We need to call `loadRelationsDynamic` equivalent but for "Find WhereIn ID".
		// We can use `loadHasManyDynamic` logic but without the "Foreign Key" part, just "ID".

		// We need to construct a query for this model.
		// We can't use `New[Model]` because we don't know the type at compile time.
		// We have `modelInstance` (empty struct).

		modelType := reflect.TypeOf(modelInstance)

		// Determine sub-relations for this type
		var subRelations []string
		if typeMap != nil {
			subRelations = typeMap[typeName]
		}

		// Fetch this type's rows by primary key. loadRelationQuery builds the
		// same `SELECT * FROM table WHERE pk IN (…)` and — because it is its own
		// function — closes its rows before the next type is queried, instead of
		// stacking one deferred Close per morph type on this loop.
		relatedInfo := ParseModelType(modelType)

		relatedResults, err := m.loadRelationQuery(ctx, relatedInfo, relatedInfo.PrimaryKey, ids, "", relatedInfo.TableName, nil)
		if err != nil {
			return err
		}

		// Recursive Load
		if len(subRelations) > 0 && len(relatedResults) > 0 {
			if err := m.loadRelationsDynamic(ctx, relatedResults, modelType, subRelations); err != nil {
				return err
			}
		}

		// 4. Map back
		pkFieldInfo := relatedInfo.Columns[relatedInfo.PrimaryKey]
		for _, res := range relatedResults {
			val := reflect.ValueOf(res).Elem()
			resID := anyToKeyString(val.FieldByIndex(pkFieldInfo.Index).Interface())

			// Find parents
			if indices, ok := parentMap[typeName][resID]; ok {
				for _, idx := range indices {
					parent := results[idx]
					parentVal := reflect.ValueOf(parent).Elem()
					relField := parentVal.FieldByName(relName)
					if relField.IsValid() && relField.CanSet() {
						relField.Set(reflect.ValueOf(res)) // Set pointer? res is *T (any)
						// If field is interface{}, it works.
					}
				}
			}
		}
	}

	return nil
}

func (m *Model[T]) loadHasMany(ctx context.Context, results []*T, relConfig any, relName string, cols string, subRelations []string, constraints *relationConstraints) error {
	// 1. Get IDs from results
	ids := make([]any, len(results))
	pkField := m.modelInfo.PrimaryKey

	// Look up field info once outside the loop
	pkFieldInfo, hasPKField := m.modelInfo.Columns[pkField]

	for i, res := range results {
		val := reflect.ValueOf(res).Elem()
		if hasPKField {
			fVal := val.FieldByIndex(pkFieldInfo.Index)
			if fVal.Kind() == reflect.Pointer {
				if fVal.IsNil() {
					ids[i] = nil
				} else {
					ids[i] = fVal.Elem().Interface()
				}
			} else {
				ids[i] = fVal.Interface()
			}
		} else {
			ids[i] = val.FieldByName("ID").Interface()
		}
	}

	// 2. Get Related Model Instance
	rel, ok := relConfig.(Relation)
	if !ok {
		return fmt.Errorf("invalid relation config")
	}

	relatedPtr := rel.NewRelated()
	relatedType := reflect.TypeOf(relatedPtr).Elem()
	relatedInfo := ParseModelType(relatedType)

	// 3. Build Query
	valConfig := reflect.ValueOf(relConfig)
	if valConfig.Kind() == reflect.Ptr {
		valConfig = valConfig.Elem()
	}
	foreignKey := valConfig.FieldByName("ForeignKey").String()
	if foreignKey == "" {
		foreignKey = ToSnakeCase(m.modelInfo.Type.Name()) + "_id"
	}

	// Extract Table Name
	relTable := valConfig.FieldByName("Table").String()

	// Use shared helper
	relatedResults, err := m.loadRelationQuery(ctx, relatedInfo, foreignKey, ids, cols, relTable, constraints)
	if err != nil {
		return err
	}

	// 4.5 Recursive Loading
	if len(subRelations) > 0 && len(relatedResults) > 0 {
		if err := m.loadRelationsDynamic(ctx, relatedResults, relatedType, subRelations); err != nil {
			return err
		}
	}

	// 5. Map back to parents
	relatedMap := make(map[string][]reflect.Value, len(relatedResults))

	// Look up field info once outside the loop
	fkFieldInfo, hasFKField := relatedInfo.Columns[foreignKey]
	if !hasFKField {
		return fmt.Errorf("foreign key column %s not found in related model", foreignKey)
	}

	for _, res := range relatedResults {
		val := reflect.ValueOf(res).Elem()
		fkKey := anyToKeyString(val.FieldByIndex(fkFieldInfo.Index).Interface())
		relatedMap[fkKey] = append(relatedMap[fkKey], reflect.ValueOf(res))
	}

	for i, parent := range results {
		parentVal := reflect.ValueOf(parent).Elem()
		parentID := anyToKeyString(ids[i])

		if children, ok := relatedMap[parentID]; ok {
			relField := m.modelInfo.GetRelationField(parentVal, relName)
			if relField.IsValid() && relField.CanSet() {
				setRelationValue(relField, children)
			}
		}
	}

	return nil
}

// setRelationValue assigns loaded child records to a relation field.
// Slice fields (HasMany) receive all children; pointer or struct fields
// (HasOne) receive the first child.
func setRelationValue(relField reflect.Value, children []reflect.Value) {
	if len(children) == 0 {
		return
	}
	switch relField.Kind() {
	case reflect.Slice:
		sliceType := relField.Type()
		slice := reflect.MakeSlice(sliceType, 0, len(children))
		for _, child := range children {
			if sliceType.Elem().Kind() == reflect.Pointer {
				slice = reflect.Append(slice, child)
			} else {
				slice = reflect.Append(slice, child.Elem())
			}
		}
		relField.Set(slice)
	case reflect.Pointer:
		relField.Set(children[0])
	default:
		relField.Set(children[0].Elem())
	}
}

func (m *Model[T]) loadBelongsToMany(ctx context.Context, results []*T, relConfig any, relName string, cols string, subRelations []string, constraints *relationConstraints) error {
	// 1. Get Relation Config
	valConfig := reflect.ValueOf(relConfig)
	if valConfig.Kind() == reflect.Ptr {
		valConfig = valConfig.Elem()
	}

	pivotTable := valConfig.FieldByName("PivotTable").String()
	foreignKey := valConfig.FieldByName("ForeignKey").String()
	relatedKey := valConfig.FieldByName("RelatedKey").String()
	localKey := valConfig.FieldByName("LocalKey").String()
	relatedPK := valConfig.FieldByName("RelatedPK").String()

	if pivotTable == "" {
		return fmt.Errorf("BelongsToMany requires PivotTable")
	}

	// 2. Get IDs from results
	ids := make([]any, len(results))
	pkField := m.modelInfo.PrimaryKey

	// Determine Local Key
	localKeyField := pkField
	if localKey != "" {
		if field, ok := m.modelInfo.Columns[localKey]; ok {
			localKeyField = field.Name
		} else {
			localKeyField = localKey // Assume it's a struct field name
		}
	} else {
		if field, ok := m.modelInfo.Columns[pkField]; ok {
			localKeyField = field.Name
		}
	}

	for i, res := range results {
		val := reflect.ValueOf(res).Elem()
		fVal := val.FieldByName(localKeyField)
		if fVal.IsValid() {
			ids[i] = fVal.Interface()
		} else {
			ids[i] = nil
		}
	}

	// Default keys
	if foreignKey == "" {
		foreignKey = ToSnakeCase(m.modelInfo.Type.Name()) + "_id"
	}
	if relatedKey == "" {
		// We need to know the related model type name
		// We can get it from relConfig (BelongsToMany[R])
		rel, ok := relConfig.(Relation)
		if !ok {
			return fmt.Errorf("BelongsToMany: expected Relation interface, got %T", relConfig)
		}
		relatedPtr := rel.NewRelated()
		relatedType := reflect.TypeOf(relatedPtr).Elem()
		relatedKey = ToSnakeCase(relatedType.Name()) + "_id"
	}

	// 3. Query Pivot Table to get Related IDs
	// SELECT foreign_key, related_key FROM pivot_table WHERE foreign_key IN (...)
	var pivotSb strings.Builder
	pivotSb.WriteString("SELECT ")
	pivotSb.WriteString(foreignKey)
	pivotSb.WriteString(", ")
	pivotSb.WriteString(relatedKey)
	pivotSb.WriteString(" FROM ")
	pivotSb.WriteString(pivotTable)
	pivotSb.WriteString(" WHERE ")
	inFrag, args, err := buildInClause(foreignKey, ids, m.effectiveDialect())
	if err != nil {
		return err
	}
	pivotSb.WriteString(inFrag)

	rows, err := m.queryer().QueryContext(ctx, rebind(pivotSb.String()), args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Map: ForeignID (string key) -> []RelatedID (string keys)
	pivotMap := make(map[string][]string, len(results))
	allRelatedIDs := make([]any, 0, len(results))
	relatedIDSet := make(map[string]bool, len(results))

	for rows.Next() {
		var fID, rID any
		if err := rows.Scan(&fID, &rID); err != nil {
			return err
		}
		fKey := anyToKeyString(fID)
		rKey := anyToKeyString(rID)
		pivotMap[fKey] = append(pivotMap[fKey], rKey)
		if !relatedIDSet[rKey] {
			relatedIDSet[rKey] = true
			allRelatedIDs = append(allRelatedIDs, rID)
		}
	}

	// Check for errors from row iteration
	if err := rows.Err(); err != nil {
		return err
	}

	if len(allRelatedIDs) == 0 {
		return nil
	}

	// 4. Query Related Model
	rel, ok := relConfig.(Relation)
	if !ok {
		return fmt.Errorf("BelongsToMany: expected Relation interface, got %T", relConfig)
	}
	relatedPtr := rel.NewRelated()
	relatedType := reflect.TypeOf(relatedPtr).Elem()
	relatedInfo := ParseModelType(relatedType)

	// Determine JOIN Key in Related Model (usually PK)
	joinKey := relatedPK
	if joinKey == "" {
		joinKey = relatedInfo.PrimaryKey
	}

	// Extract Table Name if overriden
	relTable := valConfig.FieldByName("Table").String()

	relatedResults, err := m.loadRelationQuery(ctx, relatedInfo, joinKey, allRelatedIDs, cols, relTable, constraints)
	if err != nil {
		return err
	}

	// 5. Recursive Load
	if len(subRelations) > 0 && len(relatedResults) > 0 {
		if err := m.loadRelationsDynamic(ctx, relatedResults, relatedType, subRelations); err != nil {
			return err
		}
	}

	// 6. Map back to parents
	// Map: RelatedID (string key) -> RelatedInstance
	relatedIdxMap := make(map[string]reflect.Value, len(relatedResults))
	joinKeyFieldInfo := relatedInfo.Columns[joinKey]
	if joinKeyFieldInfo == nil {
		return fmt.Errorf("join key column %s not found in related model", joinKey)
	}
	for _, res := range relatedResults {
		val := reflect.ValueOf(res).Elem()
		rID := val.FieldByIndex(joinKeyFieldInfo.Index).Interface()
		rKey := anyToKeyString(rID)
		relatedIdxMap[rKey] = reflect.ValueOf(res)
	}

	for i, parent := range results {
		parentVal := reflect.ValueOf(parent).Elem()
		parentID := ids[i]
		parentKey := anyToKeyString(parentID)

		rIDs, found := pivotMap[parentKey]
		if !found {
			continue
		}

		children := make([]reflect.Value, 0, len(rIDs))
		for _, rKey := range rIDs {
			if child, ok := relatedIdxMap[rKey]; ok {
				children = append(children, child)
			}
		}

		if len(children) > 0 {
			relField := m.modelInfo.GetRelationField(parentVal, relName)
			if relField.IsValid() && relField.CanSet() {
				setRelationValue(relField, children)
			}
		}
	}

	return nil
}

func (m *Model[T]) loadBelongsTo(ctx context.Context, results []*T, relConfig any, relName string, cols string, subRelations []string, constraints *relationConstraints) error {
	// 1. Get FKs from results (Parent.ForeignKey)
	valConfig := reflect.ValueOf(relConfig)
	if valConfig.Kind() == reflect.Ptr {
		valConfig = valConfig.Elem()
	}
	foreignKey := valConfig.FieldByName("ForeignKey").String()
	ownerKey := valConfig.FieldByName("OwnerKey").String()

	if foreignKey == "" {
		foreignKey = ToSnakeCase(relName) + "_id"
	}

	fks := make([]any, 0, len(results))
	fkMap := make(map[string][]int, len(results))  // FK string key -> []ParentIndices
	fkValues := make(map[string]any, len(results)) // FK string key -> original FK value (for query)

	for i, res := range results {
		val := reflect.ValueOf(res).Elem()

		// Find FK field in Parent (this should be "branch_id" field on User)
		var fieldName string
		if field, ok := m.modelInfo.Columns[foreignKey]; ok {
			fieldName = field.Name
		} else {
			// Fallback to CamelCase conversion
			parts := strings.Split(foreignKey, "_")
			for j, p := range parts {
				if p == "id" {
					parts[j] = "ID"
				} else {
					if len(p) > 0 {
						parts[j] = strings.ToUpper(p[:1]) + p[1:]
					}
				}
			}
			fieldName = strings.Join(parts, "")
		}

		fieldVal := val.FieldByName(fieldName)
		if !fieldVal.IsValid() {
			continue
		}

		// Handle Pointers (Nullable FKs)
		var fkVal any
		if fieldVal.Kind() == reflect.Pointer {
			if fieldVal.IsNil() {
				continue
			}
			fkVal = fieldVal.Elem().Interface()
		} else {
			fkVal = fieldVal.Interface()
		}

		if isZero(fkVal) {
			continue
		}

		fkKey := anyToKeyString(fkVal)
		if _, exists := fkMap[fkKey]; exists {
			fkMap[fkKey] = append(fkMap[fkKey], i)
		} else {
			fks = append(fks, fkVal)
			fkMap[fkKey] = []int{i}
			fkValues[fkKey] = fkVal
		}
	}

	if len(fks) == 0 {
		return nil
	}

	// 2. Get Related Model
	rel, ok := relConfig.(Relation)
	if !ok {
		return fmt.Errorf("invalid relation config")
	}

	relatedPtr := rel.NewRelated()
	relatedType := reflect.TypeOf(relatedPtr).Elem()
	relatedInfo := ParseModelType(relatedType)

	// 3. Determine the key to query on the related model (Branch)
	// OwnerKey should be the primary key of the related model (Branch.id)
	if ownerKey == "" {
		ownerKey = relatedInfo.PrimaryKey
	}

	// Extract Table Name
	relTable := valConfig.FieldByName("Table").String()

	// Use shared helper
	relatedResults, err := m.loadRelationQuery(ctx, relatedInfo, ownerKey, fks, cols, relTable, constraints)
	if err != nil {
		return err
	}

	// Recursive Load
	if len(subRelations) > 0 && len(relatedResults) > 0 {
		if err := m.loadRelationsDynamic(ctx, relatedResults, relatedType, subRelations); err != nil {
			return err
		}
	}

	relatedByPK := make(map[string]reflect.Value, len(relatedResults)) // Use string key for lookup

	// Look up field info once outside the loop
	ownerKeyFieldInfo, hasOwnerKeyField := relatedInfo.Columns[ownerKey]

	for _, res := range relatedResults {
		val := reflect.ValueOf(res).Elem()
		var resPK any
		if hasOwnerKeyField {
			resPK = val.FieldByIndex(ownerKeyFieldInfo.Index).Interface()
		} else {
			resPK = val.FieldByName("ID").Interface()
		}

		pkKey := anyToKeyString(resPK)
		relatedByPK[pkKey] = reflect.ValueOf(res)
	}

	// Now assign the related records to parent entities
	for fkKey, indices := range fkMap {
		// Find the related record with PK matching this FK value
		relatedRecord, found := relatedByPK[fkKey]
		if !found || !relatedRecord.IsValid() {
			continue
		}

		// Assign to all parent entities with this FK
		for _, idx := range indices {
			parent := results[idx]
			parentVal := reflect.ValueOf(parent).Elem()
			relField := m.modelInfo.GetRelationField(parentVal, relName)
			if relField.IsValid() && relField.CanSet() {
				if relField.Kind() == reflect.Pointer {
					relField.Set(relatedRecord)
				} else {
					relField.Set(relatedRecord.Elem())
				}
			}
		}
	}

	return nil
}

// relationConstraints holds additional query constraints extracted from a WithCallback callback.
type relationConstraints struct {
	wheres   []string
	args     []any
	orderBys []string
	limit    int
	// buildErr carries a validation failure raised inside the callback. It must
	// be surfaced, not dropped: the rejected condition is absent from the
	// generated SQL, so ignoring it loads every child rather than the filtered
	// set the caller asked for.
	buildErr error
}

// extractRelationConstraints creates a model for the related type, applies the callback,
// and extracts the accumulated query constraints.
func extractRelationConstraints(rel Relation, callback any, ctx context.Context, db *sql.DB) *relationConstraints {
	if callback == nil {
		return nil
	}

	relatedModel := rel.NewModel(ctx, db)
	if relatedModel == nil {
		return nil
	}

	// Apply callback via reflection
	fnVal := reflect.ValueOf(callback)
	if fnVal.Kind() != reflect.Func {
		return nil
	}
	fnVal.Call([]reflect.Value{reflect.ValueOf(relatedModel)})

	// Extract constraints from the populated model
	rc := &relationConstraints{}

	// A validation failure inside the callback wins over everything else.
	if getBuildErr := reflect.ValueOf(relatedModel).MethodByName("GetBuildErr"); getBuildErr.IsValid() {
		if result := getBuildErr.Call(nil); len(result) > 0 {
			if err, ok := result[0].Interface().(error); ok && err != nil {
				rc.buildErr = err
				return rc
			}
		}
	}

	// Extract wheres and args
	if getWheres := reflect.ValueOf(relatedModel).MethodByName("GetWheres"); getWheres.IsValid() {
		if result := getWheres.Call(nil); len(result) > 0 {
			if wheres, ok := result[0].Interface().([]string); ok {
				rc.wheres = wheres
			}
		}
	}
	if getArgs := reflect.ValueOf(relatedModel).MethodByName("GetArgs"); getArgs.IsValid() {
		if result := getArgs.Call(nil); len(result) > 0 {
			if args, ok := result[0].Interface().([]any); ok {
				rc.args = args
			}
		}
	}

	// Extract orderBys and limit via exported getter methods
	modelReflect := reflect.ValueOf(relatedModel)
	if getOrderBys := modelReflect.MethodByName("GetOrderBys"); getOrderBys.IsValid() {
		if result := getOrderBys.Call(nil); len(result) > 0 {
			if obs, ok := result[0].Interface().([]string); ok {
				rc.orderBys = obs
			}
		}
	}
	if getLimit := modelReflect.MethodByName("GetLimit"); getLimit.IsValid() {
		if result := getLimit.Call(nil); len(result) > 0 {
			rc.limit = int(result[0].Int())
		}
	}

	return rc
}

// buildRelationQuery assembles the SELECT an eager-load runs for one relation.
//
// A Limit set inside a WithCallback constrains the children of each parent, not
// the batch as a whole: eager loading fetches every parent's children in one
// query, so a plain LIMIT would hand the whole budget to whichever parents sort
// first and leave the rest with nothing. The limit is therefore applied with a
// ROW_NUMBER() window partitioned by the key the children are matched on, and
// the callback's ORDER BY decides which children survive. The outer query orders
// by the row number so each parent's children stay in that order.
//
// whereBody is the WHERE clause without the keyword, already carrying the IN
// fragment and any callback conditions.
func buildRelationQuery(cols, table, whereBody, partitionKey string, constraints *relationConstraints) string {
	selectList := cols
	if selectList == "" {
		selectList = "*"
	}

	var orderBy string
	if constraints != nil && len(constraints.orderBys) > 0 {
		orderBy = " ORDER BY " + strings.Join(constraints.orderBys, ", ")
	}

	var sb strings.Builder

	if constraints == nil || constraints.limit <= 0 {
		sb.WriteString("SELECT ")
		sb.WriteString(selectList)
		sb.WriteString(" FROM ")
		sb.WriteString(table)
		sb.WriteString(" WHERE ")
		sb.WriteString(whereBody)
		sb.WriteString(orderBy)
		return sb.String()
	}

	sb.WriteString("SELECT * FROM (SELECT ")
	sb.WriteString(selectList)
	sb.WriteString(", ROW_NUMBER() OVER (PARTITION BY ")
	sb.WriteString(partitionKey)
	sb.WriteString(orderBy)
	sb.WriteString(") AS zorm_row_num FROM ")
	sb.WriteString(table)
	sb.WriteString(" WHERE ")
	sb.WriteString(whereBody)
	sb.WriteString(") AS zorm_ranked WHERE zorm_row_num <= ")
	sb.WriteString(strconv.Itoa(constraints.limit))
	sb.WriteString(" ORDER BY zorm_row_num")
	return sb.String()
}

// loadRelationQuery executes a SELECT * FROM table WHERE key IN (ids)
func (m *Model[T]) loadRelationQuery(ctx context.Context, relatedInfo *ModelInfo, key string, ids []any, cols string, tableName string, constraints *relationConstraints) ([]any, error) {
	// Validate column name to prevent SQL injection
	if err := ValidateColumnName(key); err != nil {
		return nil, fmt.Errorf("invalid relation key column: %w", err)
	}

	inFrag, args, err := buildInClause(key, ids, m.effectiveDialect())
	if err != nil {
		return nil, err
	}

	table := tableName
	if table == "" {
		table = relatedInfo.TableName
	}

	var where strings.Builder
	where.WriteString(inFrag)

	// Apply callback constraints
	if constraints != nil {
		for _, w := range constraints.wheres {
			where.WriteString(" ")
			where.WriteString(w)
		}
		args = append(args, constraints.args...)
	}

	query := buildRelationQuery(cols, table, where.String(), key, constraints)

	rows, err := m.queryer().QueryContext(ctx, rebind(query), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return m.scanRowsDynamic(rows, relatedInfo)
}

func (m *Model[T]) loadMorphOneOrMany(ctx context.Context, results []*T, relConfig any, relName string, cols string, subRelations []string, constraints *relationConstraints) error {
	// 1. Get IDs from results
	ids := make([]any, len(results))
	pkField := m.modelInfo.PrimaryKey

	// Look up field info once outside the loop
	pkFieldInfo, hasPKField := m.modelInfo.Columns[pkField]

	for i, res := range results {
		val := reflect.ValueOf(res).Elem()
		if hasPKField {
			fVal := val.FieldByIndex(pkFieldInfo.Index)
			if fVal.Kind() == reflect.Pointer {
				if fVal.IsNil() {
					ids[i] = nil
				} else {
					ids[i] = fVal.Elem().Interface()
				}
			} else {
				ids[i] = fVal.Interface()
			}
		} else {
			ids[i] = val.FieldByName("ID").Interface()
		}
	}

	// 2. Get Related Model Instance
	rel, ok := relConfig.(Relation)
	if !ok {
		return fmt.Errorf("invalid relation config")
	}

	relatedPtr := rel.NewRelated()
	relatedType := reflect.TypeOf(relatedPtr).Elem()
	relatedInfo := ParseModelType(relatedType)

	// 3. Build Query
	valConfig := reflect.ValueOf(relConfig)
	if valConfig.Kind() == reflect.Ptr {
		valConfig = valConfig.Elem()
	}
	typeColumn := valConfig.FieldByName("Type").String()
	idColumn := valConfig.FieldByName("ID").String()

	if typeColumn == "" || idColumn == "" {
		return fmt.Errorf("MorphOne/MorphMany requires Type and ID columns")
	}

	// Validate column names to prevent SQL injection
	if err := ValidateColumnName(typeColumn); err != nil {
		return fmt.Errorf("invalid morph type column: %w", err)
	}
	if err := ValidateColumnName(idColumn); err != nil {
		return fmt.Errorf("invalid morph id column: %w", err)
	}

	// Determine the morph type value written into the type column. ModelInfo is
	// the single source of truth: a MorphType() method on the model, else the
	// Go struct name. MorphTo's TypeMap must be keyed by the same values.
	parentType := m.modelInfo.MorphType

	// Extract Table Name
	relTable := valConfig.FieldByName("Table").String()
	tableName := relatedInfo.TableName
	if relTable != "" {
		tableName = relTable
	}

	inFrag, inArgs, err := buildInClause(idColumn, ids, m.effectiveDialect())
	if err != nil {
		return err
	}

	var where strings.Builder
	where.WriteString(typeColumn)
	where.WriteString(" = ? AND ")
	where.WriteString(inFrag)

	args := make([]any, 0, len(inArgs)+1)
	args = append(args, parentType)
	args = append(args, inArgs...)

	// Apply callback constraints
	if constraints != nil {
		for _, w := range constraints.wheres {
			where.WriteString(" ")
			where.WriteString(w)
		}
		args = append(args, constraints.args...)
	}

	query := buildRelationQuery(cols, tableName, where.String(), idColumn, constraints)

	// Execute
	rows, err := m.queryer().QueryContext(ctx, rebind(query), args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	relatedResults, err := m.scanRowsDynamic(rows, relatedInfo)
	if err != nil {
		return err
	}

	// Recursive Load
	if len(subRelations) > 0 && len(relatedResults) > 0 {
		if err := m.loadRelationsDynamic(ctx, relatedResults, relatedType, subRelations); err != nil {
			return err
		}
	}

	// 4. Map back
	relatedMap := make(map[string][]reflect.Value, len(relatedResults))
	for _, res := range relatedResults {
		val := reflect.ValueOf(res).Elem()
		if field, ok := relatedInfo.Columns[idColumn]; ok {
			// Use FieldByIndex for access instead of FieldByName O(n)
			fkKey := anyToKeyString(val.FieldByIndex(field.Index).Interface())
			relatedMap[fkKey] = append(relatedMap[fkKey], reflect.ValueOf(res))
		}
	}

	for i, parent := range results {
		parentVal := reflect.ValueOf(parent).Elem()
		parentID := anyToKeyString(ids[i])

		if children, ok := relatedMap[parentID]; ok {
			relField := m.modelInfo.GetRelationField(parentVal, relName)
			if relField.IsValid() && relField.CanSet() {
				// MorphOne declares a pointer/struct field and MorphMany a slice,
				// which is exactly what setRelationValue dispatches on.
				setRelationValue(relField, children)
			}
		}
	}

	return nil
}

func isZero(v any) bool {
	if v == nil {
		return true
	}
	return reflect.ValueOf(v).IsZero()
}

// loadRelationsDynamic is a helper to load relations on a slice of any (which are *R).
func (m *Model[T]) loadRelationsDynamic(ctx context.Context, results []any, modelType reflect.Type, relations []string) error {
	// Group relations
	type relGroup struct {
		Cols string
		Subs []string
	}
	groups := make(map[string]*relGroup)

	for _, relationName := range relations {
		parts := strings.Split(relationName, ":")
		path := parts[0]
		cols := ""
		if len(parts) > 1 {
			cols = parts[1]
		}

		dotParts := strings.SplitN(path, ".", 2)
		root := dotParts[0]

		if _, ok := groups[root]; !ok {
			groups[root] = &relGroup{}
		}

		if len(dotParts) > 1 {
			sub := dotParts[1]
			if cols != "" {
				sub += ":" + cols
			}
			groups[root].Subs = append(groups[root].Subs, sub)
		} else {
			if cols != "" {
				groups[root].Cols = cols
			}
		}
	}

	for relName, group := range groups {
		// Find method on modelType
		// The relation might return *Related or Related.
		// We want []*Related.
		// relatedType is Related (struct type).
		ptrType := reflect.PointerTo(modelType)
		method, ok := ptrType.MethodByName(relName)
		if !ok {
			method, ok = modelType.MethodByName(relName)
			if !ok {
				// Try with "Relation" suffix
				method, ok = ptrType.MethodByName(relName + "Relation")
				if !ok {
					method, ok = modelType.MethodByName(relName + "Relation")
					if !ok {
						return fmt.Errorf("relation method %s not found on %v", relName, modelType)
					}
				}
			}
		}

		if len(results) == 0 {
			return nil
		}

		res0 := reflect.ValueOf(results[0])
		retVals := method.Func.Call([]reflect.Value{res0})
		relConfig := retVals[0].Interface()

		if rel, ok := relConfig.(Relation); ok {
			switch rel.RelationType() {
			case RelationHasMany, RelationHasOne:
				if err := m.loadHasManyDynamic(ctx, results, modelType, relConfig, relName, group.Cols, group.Subs); err != nil {
					return err
				}
			case RelationBelongsTo:
				if err := m.loadBelongsToDynamic(ctx, results, modelType, relConfig, relName, group.Cols, group.Subs); err != nil {
					return err
				}
			case RelationBelongsToMany:
				if err := m.loadBelongsToManyDynamic(ctx, results, modelType, relConfig, relName, group.Cols, group.Subs); err != nil {
					return err
				}
			case RelationMorphOne:
				if err := m.loadMorphOneOrManyDynamic(ctx, results, modelType, relConfig, relName, group.Cols, group.Subs); err != nil {
					return err
				}
			case RelationMorphMany:
				if err := m.loadMorphOneOrManyDynamic(ctx, results, modelType, relConfig, relName, group.Cols, group.Subs); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (m *Model[T]) loadHasManyDynamic(ctx context.Context, results []any, modelType reflect.Type, relConfig any, relName string, cols string, subRelations []string) error {
	modelInfo := ParseModelType(modelType)
	ids := make([]any, len(results))
	pkField := modelInfo.PrimaryKey

	for i, res := range results {
		val := reflect.ValueOf(res).Elem()
		if field, ok := modelInfo.Columns[pkField]; ok {
			// Use FieldByIndex for access instead of FieldByName O(n)
			ids[i] = val.FieldByIndex(field.Index).Interface()
		} else {
			ids[i] = val.FieldByName("ID").Interface()
		}
	}

	rel, ok := relConfig.(Relation)
	if !ok {
		return fmt.Errorf("loadHasManyDynamic: expected Relation interface, got %T", relConfig)
	}
	relatedPtr := rel.NewRelated()
	relatedType := reflect.TypeOf(relatedPtr).Elem()
	relatedInfo := ParseModelType(relatedType)

	valConfig := reflect.ValueOf(relConfig)
	foreignKey := valConfig.FieldByName("ForeignKey").String()
	if foreignKey == "" {
		foreignKey = ToSnakeCase(modelType.Name()) + "_id"
	}

	var sb strings.Builder
	sb.WriteString("SELECT ")
	if cols != "" {
		sb.WriteString(cols)
	} else {
		sb.WriteString("*")
	}
	sb.WriteString(" FROM ")
	sb.WriteString(relatedInfo.TableName)
	sb.WriteString(" WHERE ")
	inFrag, args, err := buildInClause(foreignKey, ids, m.effectiveDialect())
	if err != nil {
		return err
	}
	sb.WriteString(inFrag)

	// Execute
	rows, err := m.queryer().QueryContext(ctx, rebind(sb.String()), args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	relatedResults, err := m.scanRowsDynamic(rows, relatedInfo)
	if err != nil {
		return err
	}

	if len(subRelations) > 0 && len(relatedResults) > 0 {
		if err := m.loadRelationsDynamic(ctx, relatedResults, relatedType, subRelations); err != nil {
			return err
		}
	}

	relatedMap := make(map[string][]reflect.Value, len(relatedResults))
	for _, res := range relatedResults {
		val := reflect.ValueOf(res).Elem()
		if field, ok := relatedInfo.Columns[foreignKey]; ok {
			// Use FieldByIndex for access instead of FieldByName O(n)
			fkKey := anyToKeyString(val.FieldByIndex(field.Index).Interface())
			relatedMap[fkKey] = append(relatedMap[fkKey], reflect.ValueOf(res))
		}
	}

	for i, parent := range results {
		parentVal := reflect.ValueOf(parent).Elem()
		parentID := anyToKeyString(ids[i])

		if children, ok := relatedMap[parentID]; ok {
			relField := modelInfo.GetRelationField(parentVal, relName)
			if relField.IsValid() && relField.CanSet() {
				setRelationValue(relField, children)
			}
		}
	}

	return nil
}

func (m *Model[T]) loadBelongsToDynamic(ctx context.Context, results []any, modelType reflect.Type, relConfig any, relName string, cols string, subRelations []string) error {
	modelInfo := ParseModelType(modelType)

	valConfig := reflect.ValueOf(relConfig)
	if valConfig.Kind() == reflect.Ptr {
		valConfig = valConfig.Elem()
	}
	foreignKey := valConfig.FieldByName("ForeignKey").String()
	ownerKey := valConfig.FieldByName("OwnerKey").String()

	if foreignKey == "" {
		foreignKey = ToSnakeCase(relName) + "_id"
	}

	// Collect FK values from parent results
	fks := make([]any, 0, len(results))
	fkMap := make(map[string][]int, len(results))

	for i, res := range results {
		val := reflect.ValueOf(res).Elem()

		var fieldName string
		if field, ok := modelInfo.Columns[foreignKey]; ok {
			fieldName = field.Name
		} else {
			parts := strings.Split(foreignKey, "_")
			for j, p := range parts {
				if p == "id" {
					parts[j] = "ID"
				} else if len(p) > 0 {
					parts[j] = strings.ToUpper(p[:1]) + p[1:]
				}
			}
			fieldName = strings.Join(parts, "")
		}

		fieldVal := val.FieldByName(fieldName)
		if !fieldVal.IsValid() {
			continue
		}

		var fkVal any
		if fieldVal.Kind() == reflect.Pointer {
			if fieldVal.IsNil() {
				continue
			}
			fkVal = fieldVal.Elem().Interface()
		} else {
			fkVal = fieldVal.Interface()
		}

		if isZero(fkVal) {
			continue
		}

		fkKey := anyToKeyString(fkVal)
		if _, exists := fkMap[fkKey]; exists {
			fkMap[fkKey] = append(fkMap[fkKey], i)
		} else {
			fks = append(fks, fkVal)
			fkMap[fkKey] = []int{i}
		}
	}

	if len(fks) == 0 {
		return nil
	}

	rel, ok := relConfig.(Relation)
	if !ok {
		return fmt.Errorf("loadBelongsToDynamic: expected Relation interface, got %T", relConfig)
	}

	relatedPtr := rel.NewRelated()
	relatedType := reflect.TypeOf(relatedPtr).Elem()
	relatedInfo := ParseModelType(relatedType)

	if ownerKey == "" {
		ownerKey = relatedInfo.PrimaryKey
	}

	relTable := valConfig.FieldByName("Table").String()
	relatedResults, err := m.loadRelationQuery(ctx, relatedInfo, ownerKey, fks, cols, relTable, nil)
	if err != nil {
		return err
	}

	if len(subRelations) > 0 && len(relatedResults) > 0 {
		if err := m.loadRelationsDynamic(ctx, relatedResults, relatedType, subRelations); err != nil {
			return err
		}
	}

	relatedByPK := make(map[string]reflect.Value, len(relatedResults))
	ownerKeyFieldInfo, hasOwnerKeyField := relatedInfo.Columns[ownerKey]

	for _, res := range relatedResults {
		val := reflect.ValueOf(res).Elem()
		var resPK any
		if hasOwnerKeyField {
			resPK = val.FieldByIndex(ownerKeyFieldInfo.Index).Interface()
		} else {
			resPK = val.FieldByName("ID").Interface()
		}
		pkKey := anyToKeyString(resPK)
		relatedByPK[pkKey] = reflect.ValueOf(res)
	}

	for fkKey, indices := range fkMap {
		relatedRecord, found := relatedByPK[fkKey]
		if !found || !relatedRecord.IsValid() {
			continue
		}
		for _, idx := range indices {
			parent := results[idx]
			parentVal := reflect.ValueOf(parent).Elem()
			relField := modelInfo.GetRelationField(parentVal, relName)
			if relField.IsValid() && relField.CanSet() {
				if relField.Kind() == reflect.Pointer {
					relField.Set(relatedRecord)
				} else {
					relField.Set(relatedRecord.Elem())
				}
			}
		}
	}

	return nil
}

func (m *Model[T]) loadBelongsToManyDynamic(ctx context.Context, results []any, modelType reflect.Type, relConfig any, relName string, cols string, subRelations []string) error {
	modelInfo := ParseModelType(modelType)

	valConfig := reflect.ValueOf(relConfig)
	if valConfig.Kind() == reflect.Ptr {
		valConfig = valConfig.Elem()
	}

	pivotTable := valConfig.FieldByName("PivotTable").String()
	foreignKey := valConfig.FieldByName("ForeignKey").String()
	relatedKey := valConfig.FieldByName("RelatedKey").String()
	localKey := valConfig.FieldByName("LocalKey").String()
	relatedPK := valConfig.FieldByName("RelatedPK").String()

	if pivotTable == "" {
		return fmt.Errorf("BelongsToMany requires PivotTable")
	}

	// Get IDs from results
	ids := make([]any, len(results))
	pkField := modelInfo.PrimaryKey

	localKeyField := pkField
	if localKey != "" {
		if field, ok := modelInfo.Columns[localKey]; ok {
			localKeyField = field.Name
		} else {
			localKeyField = localKey
		}
	} else {
		if field, ok := modelInfo.Columns[pkField]; ok {
			localKeyField = field.Name
		}
	}

	for i, res := range results {
		val := reflect.ValueOf(res).Elem()
		fVal := val.FieldByName(localKeyField)
		if fVal.IsValid() {
			ids[i] = fVal.Interface()
		} else {
			ids[i] = nil
		}
	}

	if foreignKey == "" {
		foreignKey = ToSnakeCase(modelType.Name()) + "_id"
	}
	if relatedKey == "" {
		rel, ok := relConfig.(Relation)
		if !ok {
			return fmt.Errorf("BelongsToMany: expected Relation interface, got %T", relConfig)
		}
		relatedPtr := rel.NewRelated()
		relatedType := reflect.TypeOf(relatedPtr).Elem()
		relatedKey = ToSnakeCase(relatedType.Name()) + "_id"
	}

	// Query Pivot Table
	var pivotSb strings.Builder
	pivotSb.WriteString("SELECT ")
	pivotSb.WriteString(foreignKey)
	pivotSb.WriteString(", ")
	pivotSb.WriteString(relatedKey)
	pivotSb.WriteString(" FROM ")
	pivotSb.WriteString(pivotTable)
	pivotSb.WriteString(" WHERE ")
	inFrag, args, err := buildInClause(foreignKey, ids, m.effectiveDialect())
	if err != nil {
		return err
	}
	pivotSb.WriteString(inFrag)

	rows, err := m.queryer().QueryContext(ctx, rebind(pivotSb.String()), args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	pivotMap := make(map[string][]string, len(results))
	allRelatedIDs := make([]any, 0, len(results))
	relatedIDSet := make(map[string]bool, len(results))

	for rows.Next() {
		var fID, rID any
		if err := rows.Scan(&fID, &rID); err != nil {
			return err
		}
		fKey := anyToKeyString(fID)
		rKey := anyToKeyString(rID)
		pivotMap[fKey] = append(pivotMap[fKey], rKey)
		if !relatedIDSet[rKey] {
			relatedIDSet[rKey] = true
			allRelatedIDs = append(allRelatedIDs, rID)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if len(allRelatedIDs) == 0 {
		return nil
	}

	rel, ok := relConfig.(Relation)
	if !ok {
		return fmt.Errorf("BelongsToMany: expected Relation interface, got %T", relConfig)
	}
	relatedPtr := rel.NewRelated()
	relatedType := reflect.TypeOf(relatedPtr).Elem()
	relatedInfo := ParseModelType(relatedType)

	joinKey := relatedPK
	if joinKey == "" {
		joinKey = relatedInfo.PrimaryKey
	}

	relTable := valConfig.FieldByName("Table").String()
	relatedResults, err := m.loadRelationQuery(ctx, relatedInfo, joinKey, allRelatedIDs, cols, relTable, nil)
	if err != nil {
		return err
	}

	if len(subRelations) > 0 && len(relatedResults) > 0 {
		if err := m.loadRelationsDynamic(ctx, relatedResults, relatedType, subRelations); err != nil {
			return err
		}
	}

	relatedIdxMap := make(map[string]reflect.Value, len(relatedResults))
	joinKeyFieldInfo := relatedInfo.Columns[joinKey]
	if joinKeyFieldInfo == nil {
		return fmt.Errorf("join key column %s not found in related model", joinKey)
	}
	for _, res := range relatedResults {
		val := reflect.ValueOf(res).Elem()
		rID := val.FieldByIndex(joinKeyFieldInfo.Index).Interface()
		rKey := anyToKeyString(rID)
		relatedIdxMap[rKey] = reflect.ValueOf(res)
	}

	for i, parent := range results {
		parentVal := reflect.ValueOf(parent).Elem()
		parentID := ids[i]
		parentKey := anyToKeyString(parentID)

		rIDs, found := pivotMap[parentKey]
		if !found {
			continue
		}

		children := make([]reflect.Value, 0, len(rIDs))
		for _, rKey := range rIDs {
			if child, ok := relatedIdxMap[rKey]; ok {
				children = append(children, child)
			}
		}

		if len(children) > 0 {
			relField := modelInfo.GetRelationField(parentVal, relName)
			if relField.IsValid() && relField.CanSet() {
				setRelationValue(relField, children)
			}
		}
	}

	return nil
}

func (m *Model[T]) loadMorphOneOrManyDynamic(ctx context.Context, results []any, modelType reflect.Type, relConfig any, relName string, cols string, subRelations []string) error {
	modelInfo := ParseModelType(modelType)

	ids := make([]any, len(results))
	pkField := modelInfo.PrimaryKey

	for i, res := range results {
		val := reflect.ValueOf(res).Elem()
		if field, ok := modelInfo.Columns[pkField]; ok {
			ids[i] = val.FieldByIndex(field.Index).Interface()
		} else {
			ids[i] = val.FieldByName("ID").Interface()
		}
	}

	rel, ok := relConfig.(Relation)
	if !ok {
		return fmt.Errorf("loadMorphOneOrManyDynamic: expected Relation interface, got %T", relConfig)
	}

	relatedPtr := rel.NewRelated()
	relatedType := reflect.TypeOf(relatedPtr).Elem()
	relatedInfo := ParseModelType(relatedType)

	valConfig := reflect.ValueOf(relConfig)
	if valConfig.Kind() == reflect.Ptr {
		valConfig = valConfig.Elem()
	}
	typeColumn := valConfig.FieldByName("Type").String()
	idColumn := valConfig.FieldByName("ID").String()

	if typeColumn == "" || idColumn == "" {
		return fmt.Errorf("MorphOne/MorphMany requires Type and ID columns")
	}

	parentType := modelInfo.MorphType

	relTable := valConfig.FieldByName("Table").String()
	tableName := relatedInfo.TableName
	if relTable != "" {
		tableName = relTable
	}

	var sb strings.Builder
	sb.WriteString("SELECT ")
	if cols != "" {
		sb.WriteString(cols)
	} else {
		sb.WriteString("*")
	}
	sb.WriteString(" FROM ")
	sb.WriteString(tableName)
	sb.WriteString(" WHERE ")
	sb.WriteString(typeColumn)
	sb.WriteString(" = ? AND ")
	inFrag, inArgs, err := buildInClause(idColumn, ids, m.effectiveDialect())
	if err != nil {
		return err
	}
	sb.WriteString(inFrag)

	args := make([]any, 0, len(inArgs)+1)
	args = append(args, parentType)
	args = append(args, inArgs...)

	rows, err := m.queryer().QueryContext(ctx, rebind(sb.String()), args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	relatedResults, err := m.scanRowsDynamic(rows, relatedInfo)
	if err != nil {
		return err
	}

	if len(subRelations) > 0 && len(relatedResults) > 0 {
		if err := m.loadRelationsDynamic(ctx, relatedResults, relatedType, subRelations); err != nil {
			return err
		}
	}

	relatedMap := make(map[string][]reflect.Value, len(relatedResults))
	for _, res := range relatedResults {
		val := reflect.ValueOf(res).Elem()
		if field, ok := relatedInfo.Columns[idColumn]; ok {
			fkKey := anyToKeyString(val.FieldByIndex(field.Index).Interface())
			relatedMap[fkKey] = append(relatedMap[fkKey], reflect.ValueOf(res))
		}
	}

	for i, parent := range results {
		parentVal := reflect.ValueOf(parent).Elem()
		parentID := anyToKeyString(ids[i])

		if children, ok := relatedMap[parentID]; ok {
			relField := modelInfo.GetRelationField(parentVal, relName)
			if relField.IsValid() && relField.CanSet() {
				setRelationValue(relField, children)
			}
		}
	}

	return nil
}

// Attach inserts rows into the pivot table for a BelongsToMany relation.
// pivotData: map[any]map[string]any (RelatedID -> {Column: Value})
func (m *Model[T]) Attach(ctx context.Context, entity *T, relation string, ids []any, pivotData map[any]map[string]any) error {
	if len(ids) == 0 {
		return nil
	}

	// 1. Get Relation Config
	var t T
	methodVal := reflect.ValueOf(t).MethodByName(relation)
	if !methodVal.IsValid() {
		methodVal = reflect.ValueOf(t).MethodByName(relation + "Relation")
		if !methodVal.IsValid() {
			return fmt.Errorf("relation method %s not found", relation)
		}
	}
	retVals := methodVal.Call(nil)
	relConfig := retVals[0].Interface()

	// Check if it's a BelongsToMany struct
	valConfig := reflect.ValueOf(relConfig)
	if valConfig.Kind() == reflect.Ptr {
		valConfig = valConfig.Elem()
	}
	if !strings.Contains(valConfig.Type().String(), "BelongsToMany") {
		return WrapRelationError(relation, fmt.Sprintf("%T", t), ErrInvalidRelation)
	}
	pivotTable := valConfig.FieldByName("PivotTable").String()
	foreignKey := valConfig.FieldByName("ForeignKey").String()
	relatedKey := valConfig.FieldByName("RelatedKey").String()

	if pivotTable == "" {
		return WrapRelationError(relation, "pivot", ErrInvalidConfig)
	}

	// Validate relation identifiers to prevent SQL injection
	if err := ValidateColumnName(pivotTable); err != nil {
		return fmt.Errorf("invalid pivot table name: %w", err)
	}

	// 2. Get Parent ID
	parentVal := reflect.ValueOf(entity).Elem()
	pkField := m.modelInfo.PrimaryKey
	var parentID any
	if field, ok := m.modelInfo.Columns[pkField]; ok {
		// Use FieldByIndex for access instead of FieldByName O(n)
		parentID = parentVal.FieldByIndex(field.Index).Interface()
	} else {
		parentID = parentVal.FieldByName("ID").Interface()
	}

	if foreignKey == "" {
		foreignKey = ToSnakeCase(m.modelInfo.Type.Name()) + "_id"
	}
	if relatedKey == "" {
		return WrapRelationError(relation, "pivot", ErrInvalidConfig)
	}

	// Validate key columns
	if err := ValidateColumnName(foreignKey); err != nil {
		return fmt.Errorf("invalid foreign key name: %w", err)
	}
	if err := ValidateColumnName(relatedKey); err != nil {
		return fmt.Errorf("invalid related key name: %w", err)
	}

	// 3. Bulk Insert
	// We need to collect all columns first to ensure consistency
	// Base columns: foreignKey, relatedKey
	// Additional columns from pivotData

	// Find all unique pivot columns
	pivotColsMap := make(map[string]bool)
	for _, data := range pivotData {
		for k := range data {
			pivotColsMap[k] = true
		}
	}

	// Sorted so the same attach always emits the same INSERT column list,
	// instead of one statement shape per map iteration order.
	extraCols := sortedKeys(pivotColsMap)
	for _, k := range extraCols {
		// Validate extra column names
		if err := ValidateColumnName(k); err != nil {
			return fmt.Errorf("invalid pivot column name %q: %w", k, err)
		}
	}

	// Build Query
	var cols []string
	cols = append(cols, foreignKey, relatedKey)
	cols = append(cols, extraCols...)

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(pivotTable)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(cols, ", "))
	sb.WriteString(") VALUES ")

	var args []any
	rowPlaceholders := "(" + strings.TrimSuffix(strings.Repeat("?,", len(cols)), ",") + ")"

	for i, id := range ids {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(rowPlaceholders)

		// Add values
		args = append(args, parentID, id)

		// Add extra values
		for _, col := range extraCols {
			var val any
			if pivotData != nil {
				if data, ok := pivotData[id]; ok {
					val = data[col]
				}
			}
			args = append(args, val)
		}
	}

	// Pivot inserts are writes: always route to the primary.
	_, err := m.queryerForWrite().ExecContext(ctx, rebind(sb.String()), args...)
	return err
}

// Detach deletes rows from the pivot table.
func (m *Model[T]) Detach(ctx context.Context, entity *T, relation string, ids []any) error {
	// 1. Get Relation Config (Same as Attach)
	var t T
	methodVal := reflect.ValueOf(t).MethodByName(relation)
	if !methodVal.IsValid() {
		methodVal = reflect.ValueOf(t).MethodByName(relation + "Relation")
		if !methodVal.IsValid() {
			return fmt.Errorf("relation method %s not found", relation)
		}
	}
	retVals := methodVal.Call(nil)
	relConfig := retVals[0].Interface()

	valConfig := reflect.ValueOf(relConfig)
	if valConfig.Kind() == reflect.Ptr {
		valConfig = valConfig.Elem()
	}
	if valConfig.Type().Name() != "BelongsToMany" {
		// Strict check might fail if package name included? "zorm.BelongsToMany"
		if !strings.Contains(valConfig.Type().String(), "BelongsToMany") {
			return fmt.Errorf("relation %s is not BelongsToMany", relation)
		}
	}

	pivotTable := valConfig.FieldByName("PivotTable").String()
	foreignKey := valConfig.FieldByName("ForeignKey").String()
	relatedKey := valConfig.FieldByName("RelatedKey").String()

	if pivotTable == "" {
		return fmt.Errorf("pivot table not defined")
	}

	// Validate relation identifiers to prevent SQL injection
	if err := ValidateColumnName(pivotTable); err != nil {
		return fmt.Errorf("invalid pivot table name: %w", err)
	}

	// 2. Get Parent ID
	parentVal := reflect.ValueOf(entity).Elem()
	pkField := m.modelInfo.PrimaryKey
	var parentID any
	if field, ok := m.modelInfo.Columns[pkField]; ok {
		// Use FieldByIndex for access instead of FieldByName O(n)
		parentID = parentVal.FieldByIndex(field.Index).Interface()
	} else {
		parentID = parentVal.FieldByName("ID").Interface()
	}

	if foreignKey == "" {
		foreignKey = ToSnakeCase(m.modelInfo.Type.Name()) + "_id"
	}

	// Validate foreign key column
	if err := ValidateColumnName(foreignKey); err != nil {
		return fmt.Errorf("invalid foreign key name: %w", err)
	}

	// 3. Delete
	var sb strings.Builder
	sb.WriteString("DELETE FROM ")
	sb.WriteString(pivotTable)
	sb.WriteString(" WHERE ")
	sb.WriteString(foreignKey)
	sb.WriteString(" = ?")
	args := []any{parentID}

	if len(ids) > 0 {
		if relatedKey == "" {
			return fmt.Errorf("RelatedKey must be defined for Detach with IDs")
		}
		// Validate related key column
		if err := ValidateColumnName(relatedKey); err != nil {
			return fmt.Errorf("invalid related key name: %w", err)
		}
		sb.WriteString(" AND ")
		inFrag, inArgs, err := buildInClause(relatedKey, ids, m.effectiveDialect())
		if err != nil {
			return err
		}
		sb.WriteString(inFrag)
		args = append(args, inArgs...)
	}

	// Pivot deletes are writes: always route to the primary.
	_, err := m.queryerForWrite().ExecContext(ctx, rebind(sb.String()), args...)
	return err
}

// Sync synchronizes the association with the given IDs.
// It attaches missing IDs and detaches IDs that are not in the new list.
// pivotData: map[any]map[string]any (RelatedID -> {Column: Value})
//
// Sync reads the current pivot state and then issues a DELETE and an INSERT.
// Those three statements run in a single transaction so a failing attach cannot
// leave the association half-synced, and so the diff cannot be invalidated by a
// concurrent writer between the read and the writes. When the model is already
// bound to a transaction, that transaction is used instead of opening a new one.
func (m *Model[T]) Sync(ctx context.Context, entity *T, relation string, ids []any, pivotData map[any]map[string]any) error {
	if m.tx != nil {
		return m.syncPivot(ctx, entity, relation, ids, pivotData)
	}

	return m.Transaction(ctx, func(tx *Tx) error {
		return m.WithTx(tx).syncPivot(ctx, entity, relation, ids, pivotData)
	})
}

// syncPivot performs the read-diff-write of Sync on the receiver's connection.
// The receiver is expected to be transaction-bound; Sync arranges that.
func (m *Model[T]) syncPivot(ctx context.Context, entity *T, relation string, ids []any, pivotData map[any]map[string]any) error {
	// 1. Get Relation Config
	var t T
	methodVal := reflect.ValueOf(t).MethodByName(relation)
	if !methodVal.IsValid() {
		// Try with "Relation" suffix
		methodVal = reflect.ValueOf(t).MethodByName(relation + "Relation")
		if !methodVal.IsValid() {
			return fmt.Errorf("relation method %s not found", relation)
		}
	}
	retVals := methodVal.Call(nil)
	relConfig := retVals[0].Interface()

	valConfig := reflect.ValueOf(relConfig)
	if valConfig.Kind() == reflect.Ptr {
		valConfig = valConfig.Elem()
	}
	if !strings.Contains(valConfig.Type().String(), "BelongsToMany") {
		return fmt.Errorf("relation %s is not BelongsToMany", relation)
	}

	pivotTable := valConfig.FieldByName("PivotTable").String()
	foreignKey := valConfig.FieldByName("ForeignKey").String()
	relatedKey := valConfig.FieldByName("RelatedKey").String()

	if pivotTable == "" {
		return fmt.Errorf("pivot table not defined")
	}

	// Validate relation identifiers to prevent SQL injection
	if err := ValidateColumnName(pivotTable); err != nil {
		return fmt.Errorf("invalid pivot table name: %w", err)
	}

	// 2. Get Parent ID
	parentVal := reflect.ValueOf(entity).Elem()
	pkField := m.modelInfo.PrimaryKey
	var parentID any
	if field, ok := m.modelInfo.Columns[pkField]; ok {
		// Use FieldByIndex for access instead of FieldByName O(n)
		parentID = parentVal.FieldByIndex(field.Index).Interface()
	} else {
		parentID = parentVal.FieldByName("ID").Interface()
	}

	if foreignKey == "" {
		foreignKey = ToSnakeCase(m.modelInfo.Type.Name()) + "_id"
	}
	if relatedKey == "" {
		return fmt.Errorf("RelatedKey must be defined for Sync")
	}

	// Validate key columns
	if err := ValidateColumnName(foreignKey); err != nil {
		return fmt.Errorf("invalid foreign key name: %w", err)
	}
	if err := ValidateColumnName(relatedKey); err != nil {
		return fmt.Errorf("invalid related key name: %w", err)
	}

	// 3. Get Current IDs
	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(relatedKey)
	sb.WriteString(" FROM ")
	sb.WriteString(pivotTable)
	sb.WriteString(" WHERE ")
	sb.WriteString(foreignKey)
	sb.WriteString(" = ?")
	query := sb.String()
	// Read current pivot state from the primary: the subsequent attach/detach
	// diff must not be computed against a lagging replica.
	rows, err := m.queryerForWrite().QueryContext(ctx, rebind(query), parentID)
	if err != nil {
		return err
	}
	defer rows.Close()

	currentIDs := make(map[string]any) // string key -> original value
	for rows.Next() {
		var id any
		if err := rows.Scan(&id); err != nil {
			return err
		}
		key := anyToKeyString(id)
		currentIDs[key] = id
	}

	// 4. Determine Attach and Detach
	var toAttach []any
	var toDetach []any

	// Normalize input IDs to map for lookup
	newIDsMap := make(map[string]any, len(ids)) // string key -> original value
	for _, id := range ids {
		key := anyToKeyString(id)
		newIDsMap[key] = id
	}

	// Find toAttach (in new but not in current) - O(n) instead of O(n²)
	for key, id := range newIDsMap {
		if _, exists := currentIDs[key]; !exists {
			toAttach = append(toAttach, id)
		}
	}

	// Find toDetach (in current but not in new) - O(n) instead of O(n²)
	for key, curID := range currentIDs {
		if _, exists := newIDsMap[key]; !exists {
			toDetach = append(toDetach, curID)
		}
	}

	// 5. Execute
	if len(toDetach) > 0 {
		if err := m.Detach(ctx, entity, relation, toDetach); err != nil {
			return err
		}
	}

	if len(toAttach) > 0 {
		if err := m.Attach(ctx, entity, relation, toAttach, pivotData); err != nil {
			return err
		}
	}

	return nil
}
