package orm

import (
	"database/sql"
	"fmt"
)

type Dialect struct {
	DriverName                  string
	PlaceholderChar             string
	IncludeIndexInPlaceholder   bool
	AddTableNameInSelectColumns bool
	PlaceHolderGenerator        func(n int) []string
	QueryListTables             string
	QueryTableSchema            string
}

func getListOfTables(query string) func(db *sql.DB) ([]string, error) {
	return func(db *sql.DB) ([]string, error) {
		rows, err := db.Query(query)
		if err != nil {
			return nil, err
		}

		var tables []string
		for rows.Next() {
			var table string
			err = rows.Scan(&table)
			if err != nil {
				return nil, err
			}
			tables = append(tables, table)
		}

		return tables, nil
	}
}

type columnSpec struct {
	Name         string
	Type         string
	Nullable     bool
	DefaultValue sql.NullString
	IsPrimaryKey bool
}

func getTableSchema(query string) func(db *sql.DB, query string) ([]columnSpec, error) {
	return func(db *sql.DB, table string) ([]columnSpec, error) {
		rows, err := db.Query(fmt.Sprintf(query, table))
		if err != nil {
			return nil, err
		}

		var output []columnSpec
		for rows.Next() {
			var (
				cs          columnSpec
				nullableStr string
				hasDefault  string
				pkStr       string
			)

			err = rows.Scan(&cs.Name, &cs.Type, &nullableStr, &cs.DefaultValue.String, &hasDefault, &pkStr)
			if err != nil {
				return nil, err
			}

			cs.Nullable = nullableStr == "YES"
			cs.DefaultValue.Valid = hasDefault == "1"
			cs.IsPrimaryKey = pkStr != "0" // Primary key column has pk > 0
			output = append(output, cs)
		}

		return output, nil
	}
}

var Dialects = &struct {
	MySQL      *Dialect
	PostgreSQL *Dialect
	SQLite3    *Dialect
}{
	MySQL: &Dialect{
		DriverName:                  "mysql",
		PlaceholderChar:             "?",
		IncludeIndexInPlaceholder:   false,
		AddTableNameInSelectColumns: true,
		PlaceHolderGenerator:        questionMarks,
		QueryListTables:             "SHOW TABLES",
		QueryTableSchema:            "DESCRIBE %s",
	},

	PostgreSQL: &Dialect{
		DriverName:                  "postgres",
		PlaceholderChar:             "$",
		IncludeIndexInPlaceholder:   true,
		AddTableNameInSelectColumns: true,
		PlaceHolderGenerator:        postgresPlaceholder,
		QueryListTables:             "SELECT tablename FROM pg_tables WHERE schemaname = 'public'",
		QueryTableSchema:            "SELECT column_name AS Name, data_type AS Type, is_nullable, column_default, (column_name = (SELECT kcu.column_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = $1 LIMIT 1)) AS IsPrimaryKey FROM information_schema.columns WHERE table_name = $1",
	},

	SQLite3: &Dialect{
		DriverName:                  "sqlite3",
		PlaceholderChar:             "?",
		IncludeIndexInPlaceholder:   false,
		AddTableNameInSelectColumns: false,
		PlaceHolderGenerator:        questionMarks,
		QueryListTables:             "SELECT name FROM sqlite_schema WHERE type='table'",
		QueryTableSchema:            `SELECT name,type,"notnull","dflt_value","pk" FROM PRAGMA_TABLE_INFO('%s')`,
	},
}
