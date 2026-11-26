package zorm

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

func TestUpsertFailures_InvalidValuesAndRejectedWrites(t *testing.T) {
	ctx := context.Background()
	for _, operation := range []string{"FirstOrCreate", "UpdateOrCreate"} {
		for _, mode := range []string{"invalid new values", "invalid existing values", "insert failure", "update failure", "nil attributes"} {
			if operation == "FirstOrCreate" && (mode == "invalid existing values" || mode == "update failure") {
				continue // Existing records are returned without applying values.
			}
			t.Run(operation+"/"+mode, func(t *testing.T) {
				db := setupExDB(t)
				defer db.Close()
				m := New[ExModel]().SetDB(db)
				attributes, values := map[string]any{"name": "new"}, map[string]any{"value": 40}
				switch mode {
				case "invalid new values":
					values["value"] = "invalid"
				case "invalid existing values":
					attributes["name"], values["value"] = "A", "invalid"
				case "insert failure":
					if _, err := db.Exec(`CREATE TRIGGER reject_insert BEFORE INSERT ON ex_models BEGIN SELECT RAISE(FAIL, 'insertion rejected'); END`); err != nil {
						t.Fatal(err)
					}
				case "update failure":
					attributes["name"] = "A"
					if _, err := db.Exec(`CREATE TRIGGER reject_update BEFORE UPDATE ON ex_models BEGIN SELECT RAISE(FAIL, 'update rejected'); END`); err != nil {
						t.Fatal(err)
					}
				case "nil attributes":
					attributes, values = nil, nil
					if _, err := db.Exec(`DELETE FROM ex_models`); err != nil {
						t.Fatal(err)
					}
				}
				var entity *ExModel
				var err error
				if operation == "FirstOrCreate" {
					entity, err = m.FirstOrCreate(ctx, attributes, values)
				} else {
					entity, err = m.UpdateOrCreate(ctx, attributes, values)
				}
				if mode == "nil attributes" {
					if err != nil || entity == nil || entity.ID == 0 {
						t.Fatalf("empty attributes did not create an entity: entity=%v err=%v", entity, err)
					}
					defer ClearOriginals(entity)
					return
				}
				if err == nil || entity != nil {
					t.Fatalf("invalid upsert succeeded: entity=%v err=%v", entity, err)
				}
				var count, value int
				if err := db.QueryRow(`SELECT COUNT(*) FROM ex_models`).Scan(&count); err != nil || count != 3 {
					t.Fatalf("failed upsert inserted a row: count=%d err=%v", count, err)
				}
				if err := db.QueryRow(`SELECT value FROM ex_models WHERE name = 'A'`).Scan(&value); err != nil || value != 10 {
					t.Fatalf("failed upsert changed existing row: value=%d err=%v", value, err)
				}
			})
		}
	}
}

func TestUpsertFailures_DuplicateRetryOutcomes(t *testing.T) {
	ctx := context.Background()
	for _, operation := range []string{"FirstOrCreate", "UpdateOrCreate"} {
		for _, mode := range []string{"retry failure", "unrelated constraint", "winner found"} {
			t.Run(operation+"/"+mode, func(t *testing.T) {
				lookups := 0
				duplicate := errors.New("duplicate key conflict")
				failure := &failureDB{affected: 1}
				failure.query = func(query string) (driver.Rows, error) {
					if strings.HasPrefix(query, "INSERT") {
						return nil, duplicate
					}
					lookups++
					if lookups == 1 {
						return &failureRows{columns: []string{"id", "name", "value"}}, nil
					}
					if mode == "retry failure" {
						return nil, errDatabaseFailure
					}
					rows := &failureRows{columns: []string{"id", "name", "value"}}
					if mode == "winner found" {
						rows.values = [][]driver.Value{{int64(2), "winner", int64(10)}}
					}
					return rows, nil
				}
				m := New[ExModel]().SetDB(openFailureDB(t, failure))
				var entity *ExModel
				var err error
				if operation == "FirstOrCreate" {
					entity, err = m.FirstOrCreate(ctx, map[string]any{"name": "winner"}, map[string]any{"value": 20})
				} else {
					entity, err = m.UpdateOrCreate(ctx, map[string]any{"name": "winner"}, map[string]any{"value": 20})
				}
				if lookups != 2 {
					t.Errorf("expected exactly one retry, got %d lookups", lookups)
				}
				switch mode {
				case "retry failure":
					if !errors.Is(err, errDatabaseFailure) {
						t.Errorf("retry error lost: %v", err)
					}
				case "unrelated constraint":
					if !errors.Is(err, duplicate) || !IsDuplicateKey(err) {
						t.Errorf("unrelated unique constraint error lost: %v", err)
					}
				case "winner found":
					want := 10
					if operation == "UpdateOrCreate" {
						want = 20
					}
					if err != nil || entity == nil || entity.ID != 2 || entity.Value != want {
						t.Fatalf("wrong winner result: entity=%v err=%v", entity, err)
					}
					defer ClearOriginals(entity)
				}
			})
		}
	}
}
