package zorm

import (
	"database/sql/driver"
	"testing"
)

func TestPostgresArrayArguments_DriverCompatible(t *testing.T) {
	for _, values := range [][]any{{int64(1), int64(2)}, {"quote\"", "comma,", "slash\\"}, {true, false}, {1.5, 2.5}} {
		_, args, err := buildInClause("id", values, DialectPostgres)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := driver.DefaultParameterConverter.ConvertValue(args[0]); err != nil {
			t.Errorf("PostgreSQL array is not accepted by database/sql drivers: %v", err)
		}
	}
}
