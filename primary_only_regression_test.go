package zorm

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

type primaryOnly struct{ ID int }

func (primaryOnly) TableName() string { return "primary_only_rows" }

func TestPrimaryOnlyModel_CreatesGeneratedKeys(t *testing.T) {
	for _, operation := range []string{"Create", "CreateMany", "BulkInsert"} {
		t.Run(operation, func(t *testing.T) {
			db, err := sql.Open("sqlite3", ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Exec(`CREATE TABLE primary_only_rows (id INTEGER PRIMARY KEY AUTOINCREMENT)`); err != nil {
				t.Fatal(err)
			}
			m := New[primaryOnly]().SetDB(db)
			rows := []*primaryOnly{{}, {}}
			switch operation {
			case "Create":
				for _, row := range rows {
					if err = m.Create(context.Background(), row); err != nil {
						break
					}
				}
			case "CreateMany":
				err = m.CreateMany(context.Background(), rows)
			case "BulkInsert":
				err = m.BulkInsert(context.Background(), rows)
			}
			if err != nil {
				t.Fatalf("primary-key-only insert failed: %v", err)
			}
			if rows[0].ID == 0 || rows[1].ID == 0 || rows[0].ID == rows[1].ID {
				t.Errorf("generated keys missing or duplicated: %v %v", rows[0], rows[1])
			}
			for _, row := range rows {
				ClearOriginals(row)
			}
		})
	}
}

func TestPrimaryOnlyModel_PostgresDefaultSQL(t *testing.T) {
	query := buildBulkInsertSQL("primary_only_rows", "id", nil, 2, DialectPostgres)
	want := "INSERT INTO primary_only_rows (id) VALUES (DEFAULT), (DEFAULT) RETURNING id"
	if query != want || strings.Contains(query, "$1") {
		t.Errorf("wrong default insert: %s", query)
	}
	if cached := buildBulkInsertSQL("primary_only_rows", "id", nil, 2, DialectPostgres); cached != query {
		t.Error("default insert cache changed SQL")
	}
	explicit := buildBulkInsertSQL("primary_only_rows", "id", []string{"id"}, 2, DialectPostgres)
	if explicit == query || !strings.Contains(explicit, "$1") {
		t.Error("default and explicit primary-key statements collided in cache")
	}
}

func TestBulkInsertChunkSize_ParameterBoundaries(t *testing.T) {
	for _, item := range []struct{ columns, want int }{{0, 5000}, {1, 5000}, {13, 5000}, {14, 4681}, {65535, 1}, {65536, 1}} {
		if got := bulkInsertChunkSize(item.columns); got != item.want {
			t.Errorf("chunk size for %d columns=%d want=%d", item.columns, got, item.want)
		}
	}
}
