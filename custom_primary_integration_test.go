package zorm

import (
	"context"
	"database/sql"
	"testing"
)

type customPrimaryWithID struct {
	ID   int
	Code string
	Name string
}

func (customPrimaryWithID) TableName() string  { return "custom_primary_items" }
func (customPrimaryWithID) PrimaryKey() string { return "code" }

func TestCustomPrimaryKey_MethodOverridesConventionalID(t *testing.T) {
	info := ParseModel[customPrimaryWithID]()
	if info.PrimaryKey != "code" || !info.Columns["code"].IsPrimary || info.Columns["id"].IsPrimary {
		t.Fatalf("PrimaryKey method was overridden by ID convention: primary=%s", info.PrimaryKey)
	}
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE custom_primary_items (id INTEGER, code TEXT PRIMARY KEY, name TEXT);
		INSERT INTO custom_primary_items VALUES (10, 'A', 'before'), (20, 'B', 'other');
		CREATE TRIGGER reject_primary_rewrite BEFORE UPDATE OF code ON custom_primary_items BEGIN SELECT RAISE(FAIL, 'primary key must not be in SET'); END`)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	m := New[customPrimaryWithID]().SetDB(db)
	entity, err := m.Find(ctx, "A")
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(entity)
	entity.Name = "full update"
	if err := m.Update(ctx, entity); err != nil {
		t.Fatal(err)
	}
	entity.Name = "saved"
	if err := m.Save(ctx, entity); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := db.QueryRow(`SELECT name FROM custom_primary_items WHERE code = 'A'`).Scan(&name); err != nil || name != "saved" {
		t.Fatalf("custom key did not select intended row: name=%q err=%v", name, err)
	}
	if err := db.QueryRow(`SELECT name FROM custom_primary_items WHERE code = 'B'`).Scan(&name); err != nil || name != "other" {
		t.Fatalf("unrelated row changed: name=%q err=%v", name, err)
	}
	for i, operation := range []string{"Create", "CreateMany", "BulkInsert"} {
		created := &customPrimaryWithID{ID: 30 + i, Code: operation, Name: "created"}
		switch operation {
		case "Create":
			err = m.Create(ctx, created)
		case "CreateMany":
			err = m.CreateMany(ctx, []*customPrimaryWithID{created})
		case "BulkInsert":
			err = m.BulkInsert(ctx, []*customPrimaryWithID{created})
		}
		if err != nil {
			t.Fatalf("%s with a custom key failed: %v", operation, err)
		}
		defer ClearOriginals(created)
		fresh, err := m.Find(ctx, operation)
		if err != nil || fresh.ID != 30+i || fresh.Code != operation {
			t.Fatalf("%s lost the supplied key or ordinary ID: row=%v err=%v", operation, fresh, err)
		}
		defer ClearOriginals(fresh)
	}
}
