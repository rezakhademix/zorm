package zorm

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"
)

func TestCreateMany_SQLiteDoesNotDependOnReturningOrder(t *testing.T) {
	nextID := int64(100)
	db := openFailureDB(t, &failureDB{query: func(query string) (driver.Rows, error) {
		// A driver may return a multi-row INSERT's IDs in any order.
		if strings.Contains(query, "),(") || strings.Contains(query, "), (") {
			return &failureRows{columns: []string{"id"}, values: [][]driver.Value{{int64(102)}, {int64(101)}}}, nil
		}
		nextID++
		return &failureRows{columns: []string{"id"}, values: [][]driver.Value{{nextID}}}, nil
	}})
	m := New[saveItem]().SetDB(db)
	m.dialect = DialectSQLite
	items := []*saveItem{{Name: "first"}, {Name: "second"}}
	if err := m.CreateMany(context.Background(), items); err != nil {
		t.Fatal(err)
	}
	if items[0].ID != 101 || items[1].ID != 102 {
		t.Fatalf("returned IDs assigned to wrong entities: %d, %d", items[0].ID, items[1].ID)
	}
}
