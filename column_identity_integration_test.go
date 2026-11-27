package zorm

import (
	"context"
	"testing"

	first "github.com/rezakhademix/zorm/internal/testfixtures/columncache/first"
	second "github.com/rezakhademix/zorm/internal/testfixtures/columncache/second"
)

// Equal printed type names must not share scan destinations across import paths.
func TestColumnMapping_DistinguishesPackageIdentity(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	ctx := context.Background()
	if _, err := db.Exec(`UPDATE save_items SET name = '77' WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	_, err := New[first.User]().SetDB(db).Table("save_items").Select("id", "name").Where("id", 1).Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := New[second.User]().SetDB(db).Table("save_items").Select("id", "name").Where("id", 1).Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != 1 || rows[0].Name != "77" {
		t.Fatalf("cross-package scan used another model's layout: %+v", rows)
	}
}
