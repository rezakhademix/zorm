package zorm

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
)

type primaryKeyNoID struct {
	Code string
	Name string
}

func (primaryKeyNoID) PrimaryKey() string { return "code" }

type primaryKeyIDLast struct {
	Code string
	Name string
	ID   int
}

func (primaryKeyIDLast) PrimaryKey() string { return "code" }

type primaryKeyPointerReceiver struct {
	ID   int
	Code string
	Name string
}

func (*primaryKeyPointerReceiver) PrimaryKey() string { return "code" }

// PrimaryKeyEmbeddedFields supplies an aliased, promoted natural key.
type PrimaryKeyEmbeddedFields struct {
	Code string `zorm:"column:external_code"`
}

type primaryKeyEmbedded struct {
	PrimaryKeyEmbeddedFields
	ID   int
	Name string
}

func (primaryKeyEmbedded) PrimaryKey() string { return "external_code" }

type primaryKeyTaggedID struct {
	ID   int `zorm:"primaryKey"`
	Code string
	Name string
}

func (primaryKeyTaggedID) PrimaryKey() string { return "code" }

type primaryKeyAuto struct {
	Sequence int64 `zorm:"primaryKey"`
	ID       int
	Name     string
}

func (*primaryKeyAuto) PrimaryKey() string { return "sequence" }

type primaryKeyManual struct {
	Sequence int64
	ID       int
	Name     string
}

func (primaryKeyManual) PrimaryKey() string { return "sequence" }

type primaryKeyTagOnly struct {
	Code string `zorm:"primaryKey"`
	Name string
}

type primaryKeyAliasedID struct {
	ID   int `zorm:"column:external_id"`
	Name string
}

func (primaryKeyAliasedID) PrimaryKey() string { return "external_id" }

func TestCustomPrimaryKey_MetadataScenarios(t *testing.T) {
	tests := []struct {
		name string
		typ  reflect.Type
		key  string
		auto bool
	}{
		{"no conventional ID", reflect.TypeFor[primaryKeyNoID](), "code", false},
		{"ID before key", reflect.TypeFor[customPrimaryWithID](), "code", false},
		{"ID after key", reflect.TypeFor[primaryKeyIDLast](), "code", false},
		{"pointer receiver", reflect.TypeFor[primaryKeyPointerReceiver](), "code", false},
		{"embedded column alias", reflect.TypeFor[primaryKeyEmbedded](), "external_code", false},
		{"conflicting ID tag", reflect.TypeFor[primaryKeyTaggedID](), "code", false},
		{"auto numeric key", reflect.TypeFor[primaryKeyAuto](), "sequence", true},
		{"manual numeric key", reflect.TypeFor[primaryKeyManual](), "sequence", false},
		{"tag only control", reflect.TypeFor[primaryKeyTagOnly](), "code", false},
		{"default ID control", reflect.TypeFor[ExModel](), "id", true},
		{"aliased ID", reflect.TypeFor[primaryKeyAliasedID](), "external_id", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := ParseModelType(tt.typ)
			if info.PrimaryKey != tt.key {
				t.Fatalf("primary key=%q, want %q", info.PrimaryKey, tt.key)
			}
			primaryCount := 0
			for _, field := range info.OrderedFields {
				wantPrimary := field.Column == tt.key
				if field.IsPrimary != wantPrimary || info.Columns[field.Column] != field || info.Fields[field.Name] != field {
					t.Errorf("inconsistent field metadata for %s: %+v", field.Column, field)
				}
				if field.IsPrimary {
					primaryCount++
					if field.IsAuto != tt.auto {
						t.Errorf("auto=%v, want %v", field.IsAuto, tt.auto)
					}
				}
			}
			if primaryCount != 1 {
				t.Errorf("expected one primary field, got %d", primaryCount)
			}
			if ParseModelType(reflect.PointerTo(tt.typ)) != info || ParseModelType(tt.typ) != info {
				t.Error("repeated pointer/value parsing changed metadata")
			}
		})
	}
}

func TestCustomPrimaryKey_StringKeyPersistenceScenarios(t *testing.T) {
	t.Run("no conventional ID", func(t *testing.T) { exerciseStringPrimary[primaryKeyNoID](t, "code") })
	t.Run("ID before key", func(t *testing.T) { exerciseStringPrimary[customPrimaryWithID](t, "code") })
	t.Run("ID after key", func(t *testing.T) { exerciseStringPrimary[primaryKeyIDLast](t, "code") })
	t.Run("pointer receiver", func(t *testing.T) { exerciseStringPrimary[primaryKeyPointerReceiver](t, "code") })
	t.Run("embedded alias", func(t *testing.T) { exerciseStringPrimary[primaryKeyEmbedded](t, "external_code") })
	t.Run("conflicting tag", func(t *testing.T) { exerciseStringPrimary[primaryKeyTaggedID](t, "code") })
}

func exerciseStringPrimary[T any](t *testing.T, keyColumn string) {
	for _, mode := range []string{"direct", "cached", "transaction", "cached transaction"} {
		t.Run(mode, func(t *testing.T) { exerciseStringPrimaryMode[T](t, keyColumn, mode) })
	}
}

func exerciseStringPrimaryMode[T any](t *testing.T, keyColumn, mode string) {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TABLE method_key_rows (id INTEGER, code TEXT, external_code TEXT, name TEXT, PRIMARY KEY (` + keyColumn + `));
		CREATE TRIGGER reject_method_key_update BEFORE UPDATE OF ` + keyColumn + ` ON method_key_rows BEGIN SELECT RAISE(FAIL, 'primary key in SET'); END;`)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	m := New[T]().SetDB(db).Table("method_key_rows")
	if mode == "cached" || mode == "cached transaction" {
		cache := NewStmtCache(16)
		defer cache.Close()
		m = m.WithStmtCache(cache)
	}
	var activeTx *sql.Tx
	if mode == "transaction" || mode == "cached transaction" {
		activeTx, err = db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer activeTx.Rollback()
		m = m.WithTx(&Tx{Tx: activeTx, ctx: ctx})
	}
	reader := m.queryerForWrite()
	newEntity := func(code, name string) *T {
		entity := new(T)
		value := reflect.ValueOf(entity).Elem()
		value.FieldByName("Code").SetString(code)
		value.FieldByName("Name").SetString(name)
		if id := value.FieldByName("ID"); id.IsValid() {
			id.SetInt(10) // Deliberately shared ordinary ID: it must not identify rows.
		}
		return entity
	}
	for _, code := range []string{"A", "B"} {
		entity := newEntity(code, "original "+code)
		if err := m.Create(ctx, entity); err != nil {
			t.Fatal(err)
		}
		defer ClearOriginals(entity)
	}
	entity, err := m.Find(ctx, "A")
	if err != nil {
		t.Fatal(err)
	}
	defer ClearOriginals(entity)
	value := reflect.ValueOf(entity).Elem()
	value.FieldByName("Name").SetString("updated")
	if id := value.FieldByName("ID"); id.IsValid() {
		id.SetInt(42)
	}
	if err := m.Update(ctx, entity); err != nil {
		t.Fatal(err)
	}
	value.FieldByName("Name").SetString("selected")
	if err := m.UpdateColumns(ctx, entity, keyColumn, "name"); err != nil {
		t.Fatal(err)
	}
	value.FieldByName("Name").SetString("pending")
	if err := m.UpdateColumns(ctx, entity, keyColumn); err != nil || !m.IsDirtyField(entity, "name") {
		t.Fatalf("primary-only update cleared pending data: err=%v", err)
	}
	if id := value.FieldByName("ID"); id.IsValid() {
		id.SetInt(0)
	}
	if err := m.Save(ctx, entity); err != nil || m.HasDirtyFields(entity) {
		t.Fatalf("Save did not persist non-primary fields: dirty=%v err=%v", m.GetDirtyFields(entity), err)
	}
	var name string
	if err := reader.QueryRowContext(ctx, `SELECT name FROM method_key_rows WHERE `+keyColumn+` = 'A'`).Scan(&name); err != nil || name != "pending" {
		t.Fatalf("intended row not updated: name=%q err=%v", name, err)
	}
	if id := value.FieldByName("ID"); id.IsValid() {
		var storedID int
		if err := reader.QueryRowContext(ctx, `SELECT id FROM method_key_rows WHERE `+keyColumn+` = 'A'`).Scan(&storedID); err != nil || storedID != 0 {
			t.Fatalf("ordinary ID was excluded from Save: id=%d err=%v", storedID, err)
		}
	}
	if err := reader.QueryRowContext(ctx, `SELECT name FROM method_key_rows WHERE `+keyColumn+` = 'B'`).Scan(&name); err != nil || name != "original B" {
		t.Fatalf("another row with the same ordinary ID changed: name=%q err=%v", name, err)
	}
	for _, operation := range []string{"CreateMany", "BulkInsert"} {
		created := newEntity(operation, "batch")
		if operation == "CreateMany" {
			err = m.CreateMany(ctx, []*T{created})
		} else {
			err = m.BulkInsert(ctx, []*T{created})
		}
		if err != nil {
			t.Fatalf("%s failed: %v", operation, err)
		}
		fresh, err := m.Find(ctx, operation)
		if err != nil || reflect.ValueOf(fresh).Elem().FieldByName("Code").String() != operation {
			t.Fatalf("%s lost natural key: row=%v err=%v", operation, fresh, err)
		}
		defer ClearOriginals(fresh)
	}
	if err := m.Clone().Where(keyColumn, "A").Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Find(ctx, "A"); !errors.Is(err, ErrRecordNotFound) {
		t.Errorf("deleted custom-key row is still visible: %v", err)
	}
	if activeTx != nil {
		if err := activeTx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	var persisted int
	if err := db.QueryRow(`SELECT COUNT(*) FROM method_key_rows`).Scan(&persisted); err != nil || persisted != 3 {
		t.Errorf("unexpected committed row count: count=%d err=%v", persisted, err)
	}
}

func TestCustomPrimaryKey_AutoAndManualNumericKeys(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE numeric_key_rows (sequence INTEGER PRIMARY KEY, id INTEGER, name TEXT);
		CREATE TRIGGER reject_numeric_key_update BEFORE UPDATE OF sequence ON numeric_key_rows BEGIN SELECT RAISE(FAIL, 'primary key in SET'); END`); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	m := New[primaryKeyAuto]().SetDB(db).Table("numeric_key_rows")
	first := &primaryKeyAuto{ID: 99, Name: "first"}
	if err := m.Create(ctx, first); err != nil || first.Sequence == 0 {
		t.Fatalf("auto key was not generated: entity=%v err=%v", first, err)
	}
	defer ClearOriginals(first)
	batch := []*primaryKeyAuto{{ID: 99, Name: "second"}, {ID: 99, Name: "third"}}
	if err := m.CreateMany(ctx, batch); err != nil {
		t.Fatal(err)
	}
	bulk := []*primaryKeyAuto{{ID: 99, Name: "fourth"}}
	if err := m.BulkInsert(ctx, bulk); err != nil {
		t.Fatal(err)
	}
	seen := map[int64]bool{first.Sequence: true}
	for _, entity := range append(batch, bulk...) {
		if entity.Sequence == 0 || seen[entity.Sequence] || entity.ID != 99 {
			t.Fatalf("auto-key insert overwrote ordinary ID or reused key: %+v", entity)
		}
		seen[entity.Sequence] = true
	}
	first.Name = "updated"
	if err := m.Update(ctx, first); err != nil {
		t.Fatal(err)
	}
	first.Name, first.ID = "saved", 0
	if err := m.Save(ctx, first); err != nil {
		t.Fatal(err)
	}
	var others int
	if err := db.QueryRow(`SELECT COUNT(*) FROM numeric_key_rows WHERE sequence <> ? AND id = 99 AND name <> 'saved'`, first.Sequence).Scan(&others); err != nil || others != 3 {
		t.Fatalf("numeric key update affected other rows: count=%d err=%v", others, err)
	}
	manual := &primaryKeyManual{Sequence: 100, ID: 99, Name: "manual"}
	manualModel := New[primaryKeyManual]().SetDB(db).Table("numeric_key_rows")
	if err := manualModel.Create(ctx, manual); err != nil || manual.Sequence != 100 {
		t.Fatalf("manual numeric key was regenerated: entity=%v err=%v", manual, err)
	}
	defer ClearOriginals(manual)
	fresh, err := manualModel.Find(ctx, int64(100))
	if err != nil || fresh.Name != "manual" {
		t.Fatalf("manual numeric key lookup failed: row=%v err=%v", fresh, err)
	}
	defer ClearOriginals(fresh)
}
