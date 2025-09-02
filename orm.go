package orm

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"

	// Drivers
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
)

var globalConnections = map[string]*Connection{}

// PrintSchematic prints a visual representation of the inferred database schema
// for each connection. This is useful for debugging and understanding how the ORM
// maps your Go structs to database tables.
func PrintSchematic() {
	for name, conn := range globalConnections {
		fmt.Printf("---------------- %s ----------------\n", name)
		conn.PrintSchematic()
		fmt.Println("-----------------------------------")
	}
}

type ConnectionConfig struct {
	// Name identifies this database connection.
	// Required if you define multiple connections.
	Name string

	// An existing database connection.
	// If provided, Driver and DSN should be omitted.
	DB *sql.DB

	// SQL dialect used for query generation.
	// Typically optional when using standard databases (MySQL, SQLite, PostgreSQL).
	Dialect *Dialect

	// Entities registered with this connection.
	// Optional: if omitted, Qorm will build the metadata cache incrementally.
	// However, this means losing schema insights and certain validations.
	Entities []Entity

	// Enables schema validation:
	//   - Verifies all tables exist
	//   - Ensures tables contain required columns
	//   - Checks inferred tables are present
	DatabaseValidations bool
}

// SetupConnections configures and registers database connections.
func SetupConnections(configs ...ConnectionConfig) error {
	for _, config := range configs {
		if err := setupConnection(config); err != nil {
			return err
		}
	}

	for _, conn := range globalConnections {
		if !conn.DatabaseValidations {
			continue
		}

		tables, err := getListOfTables(conn.Dialect.QueryListTables)(conn.DB)
		if err != nil {
			return err
		}

		for _, table := range tables {
			if conn.DatabaseValidations {
				spec, err := getTableSchema(conn.Dialect.QueryTableSchema)(conn.DB, table)
				if err != nil {
					return err
				}
				conn.DBSchema[table] = spec
			} else {
				conn.DBSchema[table] = nil
			}
		}

		if conn.DatabaseValidations {
			if err := conn.validateAllTablesArePresent(); err != nil {
				return err
			}

			if err := conn.validateTablesSchemas(); err != nil {
				return err
			}
		}
	}

	return nil
}

func setupConnection(config ConnectionConfig) error {
	schemas := map[string]*schema{}
	if config.Name == "" {
		config.Name = "default"
		fmt.Printf("using default connection name: %s\n", config.Name)
	}

	for _, entity := range config.Entities {
		s := schemaOfHeavyReflectionStuff(entity)
		var configurator EntityConfigurator
		entity.ConfigureEntity(&configurator)
		schemas[configurator.table] = s
	}

	s := &Connection{
		Name:                config.Name,
		DB:                  config.DB,
		Dialect:             config.Dialect,
		Schemas:             schemas,
		DBSchema:            make(map[string][]columnSpec),
		DatabaseValidations: config.DatabaseValidations,
	}

	globalConnections[config.Name] = s

	return nil
}

// Entity represents a database entity.
// Any struct that should be mapped to a database table must implement this interface.
type Entity interface {
	// ConfigureEntity defines the entity's metadata, such as:
	//   - Table name
	//   - Database connection
	//   - Relationships
	ConfigureEntity(e *EntityConfigurator)
}

// InsertAll inserts multiple entities into the database in a single query.
func InsertAll(entities ...Entity) error {
	if len(entities) == 0 {
		return nil
	}

	schema := getSchemaFor(entities[0])
	columns := schema.Columns(false)
	values := make([][]any, 0, len(entities))

	for _, entity := range entities {
		now := sql.NullTime{Time: time.Now(), Valid: true}
		if createdAtField := schema.createdAt(); createdAtField != nil {
			genericSet(entity, createdAtField.Name, now)
		}

		if updatedAtField := schema.updatedAt(); updatedAtField != nil {
			genericSet(entity, updatedAtField.Name, now)
		}

		values = append(values, genericValuesOf(entity, false))
	}

	stmt := insertStmt{
		PlaceHolderGenerator: schema.getDialect().PlaceHolderGenerator,
		Table:                schema.getTable(),
		Columns:              columns,
		Values:               values,
	}

	query, args := stmt.ToSql()
	_, err := schema.getConnection().exec(query, args...)

	return err
}

// Insert inserts a single entity into the database.
func Insert(entity Entity) error {
	schema := getSchemaFor(entity)
	columns := schema.Columns(false)
	values := make([][]any, 0, 1)
	now := sql.NullTime{Time: time.Now(), Valid: true}

	if createdAtField := schema.createdAt(); createdAtField != nil {
		genericSet(entity, createdAtField.Name, now)
	}

	if updatedAtField := schema.updatedAt(); updatedAtField != nil {
		genericSet(entity, updatedAtField.Name, now)
	}

	values = append(values, genericValuesOf(entity, false))

	stmt := insertStmt{
		PlaceHolderGenerator: schema.getDialect().PlaceHolderGenerator,
		Table:                schema.getTable(),
		Columns:              columns,
		Values:               values,
	}

	if schema.getDialect().DriverName == "postgres" {
		stmt.Returning = schema.pkName()
	}

	query, args := stmt.ToSql()
	res, err := schema.getConnection().exec(query, args...)
	if err != nil {
		return err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return err
	}

	if schema.pkName() != "" {
		schema.setPK(entity, id)
	}

	return nil
}

func isZero(val any) bool {
	switch val := val.(type) {
	case int64:
		return val == 0
	case int:
		return val == 0
	case string:
		return val == ""
	default:
		return reflect.ValueOf(val).Elem().IsZero()
	}
}

// Save persists the given entity.
// If the primary key has a zero value, an INSERT query is executed.
// Otherwise, an UPDATE query is executed.
func Save(obj Entity) error {
	if isZero(getSchemaFor(obj).getPK(obj)) {
		return Insert(obj)
	} else {
		return Update(obj)
	}
}

// Find retrieves an entity by its primary key.
// The entity type is inferred from the generic type parameter T.
func Find[T Entity](id any) (T, error) {
	out := new(T)
	md := getSchemaFor(*out)

	q, args, err := NewQueryBuilder[T](md).
		SetDialect(md.getDialect()).
		Table(md.Table).
		Select(md.Columns(true)...).
		Where(md.pkName(), id).
		ToSql()
	if err != nil {
		return *out, err
	}

	if err = bind[T](out, q, args); err != nil {
		return *out, err
	}

	return *out, nil
}

func toKeyValues(entity Entity, withPK bool) []any {
	values := genericValuesOf(entity, withPK)
	columns := getSchemaFor(entity).Columns(withPK)
	keyValues := make([]any, 0, len(columns)*2)

	for i, col := range columns {
		keyValues = append(keyValues, col, values[i])
	}

	return keyValues
}

// Update modifies the given entity in the database.
// The update is matched on the entity's primary key.
func Update(obj Entity) error {
	s := getSchemaFor(obj)

	q, args, err := NewQueryBuilder[Entity](s).
		SetDialect(s.getDialect()).
		Set(toKeyValues(obj, false)...).
		Where(s.pkName(), genericGetPKValue(obj)).
		Table(s.Table).
		ToSql()
	if err != nil {
		return err
	}

	_, err = s.getConnection().exec(q, args...)

	return err
}

// Delete marks the given entity as deleted in the database.
// The deletion is performed by setting the "deleted_at" timestamp
// based on the entity's primary key.
func Delete(obj Entity) error {
	s := getSchemaFor(obj)

	genericSet(obj, "deleted_at", sql.NullTime{Time: time.Now(), Valid: true})
	query, args, err := NewQueryBuilder[Entity](s).
		SetDialect(s.getDialect()).
		Table(s.Table).
		Where(s.pkName(), genericGetPKValue(obj)).
		SetDelete().
		ToSql()
	if err != nil {
		return err
	}

	_, err = s.getConnection().exec(query, args...)

	return err
}

func bind[T Entity](output any, q string, args []any) error {
	outputMD := getSchemaFor(*new(T))

	rows, err := outputMD.getConnection().query(q, args...)
	if err != nil {
		return err
	}

	return newBinder(outputMD).bind(rows, output)
}

// HasManyConfig defines the configuration for a HasMany relationship.
// Qorm can usually infer these values when fields follow standard naming
// conventions, but they can be set explicitly for custom cases.
type HasManyConfig struct {
	// PropertyTable is the table name of the related entity.
	// Example: in a Post–Comment relationship (Post has many Comments),
	// PropertyTable = "comments".
	PropertyTable string

	// PropertyForeignKey is the foreign key in the related entity's table.
	// Example: in the same Post–Comment relationship, if Comment has a
	// "post_id" column, PropertyForeignKey = "post_id".
	PropertyForeignKey string
}

// HasMany configures a QueryBuilder for a HasMany relationship.
// The owner entity is expected to have many instances of the PROPERTY entity type.
// Example: HasMany[Comment](&Post{}) sets up a query for all Comments related to a Post.
func HasMany[PROPERTY Entity](owner Entity) *QueryBuilder[PROPERTY] {
	outSchema := getSchemaFor(*new(PROPERTY))

	q := NewQueryBuilder[PROPERTY](outSchema)
	// retrieve the HasMany configuration from the cache
	c, ok := getSchemaFor(owner).relations[outSchema.Table].(HasManyConfig)
	if !ok {
		q.err = fmt.Errorf("wrong config passed for HasMany")
	}

	s := getSchemaFor(owner)
	return q.
		SetDialect(s.getDialect()).
		Table(c.PropertyTable).
		Select(outSchema.Columns(true)...).
		Where(c.PropertyForeignKey, genericGetPKValue(owner))
}

// HasOneConfig defines the configuration for a HasOne relationship.
// It is similar to HasManyConfig but represents a one-to-one association.
type HasOneConfig struct {
	// PropertyTable is the table name of the related entity.
	// Example: in a Post–HeaderPicture relationship (Post has one HeaderPicture),
	// PropertyTable = "header_pictures".
	PropertyTable string

	// PropertyForeignKey is the foreign key in the related entity's table.
	// Example: in the same Post–HeaderPicture relationship, if HeaderPicture has
	// a "post_id" column, PropertyForeignKey = "post_id".
	PropertyForeignKey string
}

// HasOne configures a QueryBuilder for a HasOne relationship.
// The owner entity is expected to have exactly one instance of the PROPERTY entity type.
// Example: HasOne[HeaderPicture](&Post{}) sets up a query for the HeaderPicture related to a Post.
func HasOne[PROPERTY Entity](owner Entity) *QueryBuilder[PROPERTY] {
	property := getSchemaFor(*new(PROPERTY))
	q := NewQueryBuilder[PROPERTY](property)
	c, ok := getSchemaFor(owner).relations[property.Table].(HasOneConfig)
	if !ok {
		q.err = fmt.Errorf("wrong config passed for HasOne")
	}

	return q.
		SetDialect(property.getDialect()).
		Table(c.PropertyTable).
		Select(property.Columns(true)...).
		Where(c.PropertyForeignKey, genericGetPKValue(owner))
}

// BelongsToConfig defines the configuration for a BelongsTo relationship.
// A BelongsTo relationship represents an association where one entity belongs to another.
// Example: a Comment belongs to a Post.
type BelongsToConfig struct {
	// OwnerTable is the table of the owning entity in the relationship.
	// Example: in a Comment–Post relationship, OwnerTable = "posts".
	OwnerTable string

	// LocalForeignKey is the field in the current entity that references the owner.
	// Example: in a Comment–Post relationship, LocalForeignKey = "post_id" in Comment.
	LocalForeignKey string

	// ForeignColumnName is the column in the owner entity that LocalForeignKey references.
	// Example: in the same relationship, ForeignColumnName = "id" in Post.
	ForeignColumnName string
}

// BelongsTo configures a QueryBuilder for a BelongsTo relationship.
// The property entity is expected to belong to a single OWNER entity type.
// Example: BelongsTo[Post](&Comment{}) sets up a query to fetch the Post that a Comment belongs to.
func BelongsTo[OWNER Entity](property Entity) *QueryBuilder[OWNER] {
	owner := getSchemaFor(*new(OWNER))
	q := NewQueryBuilder[OWNER](owner)
	c, ok := getSchemaFor(property).relations[owner.Table].(BelongsToConfig)
	if !ok {
		q.err = fmt.Errorf("wrong config passed for BelongsTo")
	}

	ownerIDidx := 0
	for idx, field := range owner.fields {
		if field.Name == c.LocalForeignKey {
			ownerIDidx = idx
		}
	}

	ownerID := genericValuesOf(property, true)[ownerIDidx]

	return q.
		SetDialect(owner.getDialect()).
		Table(c.OwnerTable).
		Select(owner.Columns(true)...).
		Where(c.ForeignColumnName, ownerID)
}

// BelongsToManyConfig contains configuration for a many-to-many (BelongsToMany) relationship.
type BelongsToManyConfig struct {
	// IntermediateTable is the name of the join table linking the two entities.
	// Example: in a Post BelongsToMany Category relationship, this would be "post_categories".
	// Note: this field cannot be inferred automatically.
	IntermediateTable string

	// IntermediatePropertyID is the column in the join table that references the property entity.
	// Example: in "post_categories", this would be "post_id".
	IntermediatePropertyID string

	// IntermediateOwnerID is the column in the join table that references the owner entity.
	// Example: in "post_categories", this would be "category_id".
	IntermediateOwnerID string

	// OwnerTable is the table name of the owner entity.
	// Example: in Post BelongsToMany Category, this would be "categories".
	OwnerTable string

	// OwnerLookupColumn is the column in the owner table used for joining (usually the primary key).
	// Example: in Category, this would be "id".
	OwnerLookupColumn string
}

// BelongsToMany configures a QueryBuilder for a many-to-many (BelongsToMany) relationship.
// The property entity is expected to be linked to multiple OWNER entities through an intermediate table.
// Example: BelongsToMany[Category](&Post{}) sets up a query to fetch all Categories related to a Post.
func BelongsToMany[OWNER Entity](property Entity) *QueryBuilder[OWNER] {
	out := *new(OWNER)
	outSchema := getSchemaFor(out)
	q := NewQueryBuilder[OWNER](outSchema)
	c, ok := getSchemaFor(property).relations[outSchema.Table].(BelongsToManyConfig)
	if !ok {
		q.err = fmt.Errorf("wrong config passed for HasMany")
	}
	return q.
		Select(outSchema.Columns(true)...).
		Table(outSchema.Table).
		WhereIn(c.OwnerLookupColumn, Raw(fmt.Sprintf(`SELECT %s FROM %s WHERE %s = ?`,
			c.IntermediatePropertyID,
			c.IntermediateTable, c.IntermediateOwnerID), genericGetPKValue(property)))
}

// Add associates one or more `items` with the `to` entity using the relationship
// configuration defined in the `ConfigureEntity` method of `to`.
// It supports HasMany, HasOne, and BelongsToMany relationships.
func Add(to Entity, items ...Entity) error {
	if len(items) == 0 {
		return nil
	}

	rels := getSchemaFor(to).relations
	tname := getSchemaFor(items[0]).Table
	c, ok := rels[tname]
	if !ok {
		return fmt.Errorf("no config found for the given entity and items")
	}

	switch c.(type) {
	case HasManyConfig:
		return addProperty(to, items...)
	case HasOneConfig:
		return addProperty(to, items[0])
	case BelongsToManyConfig:
		return addM2M(to, items...)
	default:
		return fmt.Errorf("cannot add for relation: %T", rels[getSchemaFor(items[0]).Table])
	}
}

func addM2M(to Entity, items ...Entity) error {
	schema := getSchemaFor(to)
	relationConfig := schema.relations[getSchemaFor(items[0]).Table].(BelongsToManyConfig)
	values := make([][]any, 0, len(items))
	ownerPk := genericGetPKValue(to)

	for _, item := range items {
		pk := genericGetPKValue(item)
		if isZero(pk) {
			if err := Insert(item); err != nil {
				return err
			}

			pk = genericGetPKValue(item)
		}

		values = append(values, []any{ownerPk, pk})
	}

	stmt := insertStmt{
		PlaceHolderGenerator: schema.getDialect().PlaceHolderGenerator,
		Table:                relationConfig.IntermediateTable,
		Columns:              []string{relationConfig.IntermediateOwnerID, relationConfig.IntermediatePropertyID},
		Values:               values,
	}

	query, args := stmt.ToSql()
	_, err := getConnectionFor(items[0]).DB.Exec(query, args...)
	return err
}

// addProperty inserts one or more property entities into the database and
// links them to the owner entity in a HasMany or HasOne relationship.
// It ensures all items belong to the same table and sets the foreign key
// to the owner's primary key before insertion.
// Example: addProperty(Post, comments) inserts comments and links them to the given Post.
func addProperty(to Entity, items ...Entity) error {
	var lastTable string
	for _, obj := range items {
		s := getSchemaFor(obj)
		if lastTable == "" {
			lastTable = s.Table
		} else {
			if lastTable != s.Table {
				return fmt.Errorf("cannot batch insert for two different tables: %s and %s", s.Table, lastTable)
			}
		}
	}
	i := insertStmt{
		PlaceHolderGenerator: getSchemaFor(to).getDialect().PlaceHolderGenerator,
		Table:                getSchemaFor(items[0]).getTable(),
	}
	ownerPKIdx := -1
	ownerPKName := getSchemaFor(items[0]).relations[getSchemaFor(to).Table].(BelongsToConfig).LocalForeignKey
	for idx, col := range getSchemaFor(items[0]).Columns(false) {
		if col == ownerPKName {
			ownerPKIdx = idx
		}
	}

	ownerPK := genericGetPKValue(to)
	if ownerPKIdx != -1 {
		cols := getSchemaFor(items[0]).Columns(false)
		i.Columns = append(i.Columns, cols...)

		for _, item := range items {
			vals := genericValuesOf(item, false)
			if cols[ownerPKIdx] != getSchemaFor(items[0]).relations[getSchemaFor(to).Table].(BelongsToConfig).LocalForeignKey {
				return fmt.Errorf("owner pk idx is not correct")
			}
			vals[ownerPKIdx] = ownerPK
			i.Values = append(i.Values, vals)
		}
	} else {
		ownerPKIdx = 0
		cols := getSchemaFor(items[0]).Columns(false)
		cols = append(cols[:ownerPKIdx+1], cols[ownerPKIdx:]...)
		cols[ownerPKIdx] = getSchemaFor(items[0]).relations[getSchemaFor(to).Table].(BelongsToConfig).LocalForeignKey
		i.Columns = append(i.Columns, cols...)
		for _, item := range items {
			vals := genericValuesOf(item, false)
			if cols[ownerPKIdx] != getSchemaFor(items[0]).relations[getSchemaFor(to).Table].(BelongsToConfig).LocalForeignKey {
				return fmt.Errorf("owner pk idx is not correct")
			}
			vals = append(vals[:ownerPKIdx+1], vals[ownerPKIdx:]...)
			vals[ownerPKIdx] = ownerPK
			i.Values = append(i.Values, vals)
		}
	}

	q, args := i.ToSql()

	_, err := getConnectionFor(items[0]).DB.Exec(q, args...)
	if err != nil {
		return err
	}

	return err
}

// Query creates a new QueryBuilder for the given entity type.
// It automatically sets the SQL dialect and table name based on the entity's schema.
func Query[E Entity]() *QueryBuilder[E] {
	s := getSchemaFor(*new(E))
	q := NewQueryBuilder[E](s)
	q.SetDialect(s.getDialect()).Table(s.Table)

	return q
}

// ExecRaw executes a raw SQL query on the database for the given entity type.
// It returns the last inserted ID, the number of affected rows, and any error encountered.
func ExecRaw[E Entity](q string, args ...any) (int64, int64, error) {
	e := new(E)

	res, err := getSchemaFor(*e).getSQLDB().Exec(q, args...)
	if err != nil {
		return 0, 0, err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, 0, err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, 0, err
	}

	return id, affected, nil
}

// QueryRaw executes a raw SQL query on the database for the given entity type
// and returns a slice of results mapped to the OUTPUT entity type.
func QueryRaw[OUTPUT Entity](q string, args ...any) ([]OUTPUT, error) {
	o := new(OUTPUT)
	rows, err := getSchemaFor(*o).getSQLDB().Query(q, args...)
	if err != nil {
		return nil, err
	}

	var output []OUTPUT
	err = newBinder(getSchemaFor(*o)).bind(rows, &output)
	if err != nil {
		return nil, err
	}

	return output, nil
}
