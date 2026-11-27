package zorm

import (
	"context"
	"database/sql"
	"testing"
)

type IgnoredEmbeddedFields struct {
	Secret   string
	Revision int64 `zorm:"version"`
}

type ignoredEmbeddedRecord struct {
	ID                    int
	IgnoredEmbeddedFields `zorm:"-"`
	Name                  string
}

func (ignoredEmbeddedRecord) TableName() string { return "ignored_embedded_records" }

type IgnoredEmbeddedContainer struct {
	IgnoredEmbeddedFields `zorm:"-"`
	Visible               string
}

type nestedIgnoredEmbeddedRecord struct {
	ID int
	IgnoredEmbeddedContainer
}

// An ignored embedded field must not contribute columns or a version field.
func TestIgnoredEmbedded_ExcludedFromMetadata(t *testing.T) {
	for _, info := range []*ModelInfo{ParseModel[ignoredEmbeddedRecord](), ParseModel[nestedIgnoredEmbeddedRecord]()} {
		for _, column := range []string{"secret", "revision"} {
			if _, ok := info.Columns[column]; ok {
				t.Errorf("%s includes ignored column %s", info.Type, column)
			}
		}
		if info.VersionField != nil {
			t.Errorf("%s includes an ignored version field", info.Type)
		}
	}
	if _, ok := ParseModel[nestedIgnoredEmbeddedRecord]().Columns["visible"]; !ok {
		t.Fatal("non-ignored embedded sibling was excluded")
	}
}

func TestIgnoredEmbedded_Persistence(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE ignored_embedded_records (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	m := New[ignoredEmbeddedRecord]().SetDB(db)
	entity := &ignoredEmbeddedRecord{IgnoredEmbeddedFields: IgnoredEmbeddedFields{Secret: "private", Revision: 99}, Name: "created"}
	if err := m.Create(ctx, entity); err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(entity)
	entity.Name = "saved"
	if err := m.Save(ctx, entity); err != nil {
		t.Fatal(err)
	}
	entity.Name = "updated"
	if err := m.Update(ctx, entity); err != nil {
		t.Fatal(err)
	}
	loaded, err := m.Find(ctx, entity.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(loaded)
	if loaded.Name != "updated" || loaded.Secret != "" || loaded.Revision != 0 {
		t.Fatalf("unexpected loaded record: %+v", loaded)
	}
}
