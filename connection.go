package orm

import (
	"database/sql"
	"fmt"

	"github.com/jedib0t/go-pretty/table"
)

type Connection struct {
	Name                string
	Dialect             *Dialect
	DB                  *sql.DB
	Schemas             map[string]*schema
	DBSchema            map[string][]columnSpec
	DatabaseValidations bool
}

// InferredTables collects every database table name defined in the connection schemas.
// This includes both entity tables and intermediate (pivot) tables from BelongsToMany relations.
// The order of returned tables is not guaranteed.
func (c *Connection) InferredTables() []string {
	count := len(c.Schemas)

	for _, s := range c.Schemas {
		for _, relC := range s.relations {
			if _, is := relC.(BelongsToManyConfig); is {
				count++
			}
		}
	}

	tables := make([]string, 0, count)
	for t, s := range c.Schemas {
		tables = append(tables, t)
		for _, relC := range s.relations {
			if belongsToManyConfig, is := relC.(BelongsToManyConfig); is {
				tables = append(tables, belongsToManyConfig.IntermediateTable)
			}
		}
	}

	return tables
}

// validateAllTablesArePresent checks all inferred tables exist in the database schema
func (c *Connection) validateAllTablesArePresent() error {
	for _, inferredTable := range c.InferredTables() {
		if _, exists := c.DBSchema[inferredTable]; !exists {
			return fmt.Errorf("ORM inferred %s but it's not found in your database, database is out of sync", inferredTable)
		}
	}

	return nil
}

func (c *Connection) validateTablesSchemas() error {
	for table, sc := range c.Schemas {
		if columns, exists := c.DBSchema[table]; exists {
			for _, f := range sc.fields {
				found := false
				for _, c := range columns {
					if c.Name == f.Name {
						found = true
					}
				}

				if !found {
					return fmt.Errorf("column %s not found while it was inferred", f.Name)
				}
			}
		} else {
			return fmt.Errorf("tables are out of sync, %s was inferred but not present in database", table)
		}
	}

	// check for relation tables: for HasMany,HasOne relations check if OWNER pk column is in PROPERTY,
	// for BelongsToMany check intermediate table has 2 pk for two entities

	for table, sc := range c.Schemas {
		for _, rel := range sc.relations {
			switch rel := rel.(type) {
			case BelongsToConfig:
				columns := c.DBSchema[table]
				var found bool
				for _, col := range columns {
					if col.Name == rel.LocalForeignKey {
						found = true
					}
				}
				if !found {
					return fmt.Errorf("cannot find local foreign key %s for relation", rel.LocalForeignKey)
				}
			case BelongsToManyConfig:
				columns := c.DBSchema[rel.IntermediateTable]
				var foundOwner bool
				var foundProperty bool

				for _, col := range columns {
					if col.Name == rel.IntermediateOwnerID {
						foundOwner = true
					}
					if col.Name == rel.IntermediatePropertyID {
						foundProperty = true
					}
				}
				if !foundOwner || !foundProperty {
					return fmt.Errorf("table schema for %s is not correct one of foreign keys is not present", rel.IntermediateTable)
				}
			}
		}
	}

	return nil
}

func (c *Connection) PrintSchematic() {
	fmt.Printf("SQL Dialect: %s\n", c.Dialect.DriverName)
	for t, schema := range c.Schemas {
		fmt.Printf("t: %s\n", t)
		w := table.NewWriter()
		w.AppendHeader(table.Row{"SQL Name", "Type", "Is Primary Key", "Is Virtual"})
		for _, field := range schema.fields {
			w.AppendRow(table.Row{field.Name, field.Type, field.IsPK, field.Virtual})
		}
		fmt.Println(w.Render())
		for _, rel := range schema.relations {
			switch rel := rel.(type) {
			case HasOneConfig:
				fmt.Printf("%s 1-1 %s => %+v\n", t, rel.PropertyTable, rel)
			case HasManyConfig:
				fmt.Printf("%s 1-N %s => %+v\n", t, rel.PropertyTable, rel)

			case BelongsToConfig:
				fmt.Printf("%s N-1 %s => %+v\n", t, rel.OwnerTable, rel)

			case BelongsToManyConfig:
				fmt.Printf("%s N-N %s => %+v\n", t, rel.IntermediateTable, rel)
			}
		}

		fmt.Println("")
	}
}

func (c *Connection) getSchema(t string) *schema {
	return c.Schemas[t]
}

func (c *Connection) setSchema(e Entity, s *schema) {
	var configurator EntityConfigurator
	e.ConfigureEntity(&configurator)

	c.Schemas[configurator.table] = s
}

func GetConnection(name string) *Connection {
	return globalConnections[name]
}

func (c *Connection) exec(q string, args ...any) (sql.Result, error) {
	return c.DB.Exec(q, args...)
}

func (c *Connection) query(q string, args ...any) (*sql.Rows, error) {
	return c.DB.Query(q, args...)
}

func (c *Connection) queryRow(q string, args ...any) *sql.Row {
	return c.DB.QueryRow(q, args...)
}
