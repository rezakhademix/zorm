package zorm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"
	"weak"
)

func TestNoDatabase_PublicWriteGuardsAndFallbacks(t *testing.T) {
	oldAtomic, oldLegacy, oldResolver := globalDB.Load(), GlobalDB, GetGlobalResolver()
	defer func() { globalDB.Store(oldAtomic); GlobalDB = oldLegacy; globalResolver.Store(oldResolver) }()
	SetGlobalDB(nil)
	ClearDBResolver()
	ctx := context.Background()
	m := New[ExModel]()
	if m.queryer().(*sql.DB) != nil || m.queryerForWrite().(*sql.DB) != nil || Query[int]().queryer().(*sql.DB) != nil {
		t.Error("unconfigured queryer should resolve to a nil database")
	}
	if err := Transaction(ctx, func(*Tx) error { t.Error("callback without DB"); return nil }); !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("global transaction without DB: %v", err)
	}
	if err := m.Transaction(ctx, func(*Tx) error { t.Error("callback without DB"); return nil }); !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("model transaction without DB: %v", err)
	}
	if err := m.withAutoTx(ctx, func(*Model[ExModel]) error { t.Error("callback without DB"); return nil }); !errors.Is(err, ErrNilDatabase) {
		t.Errorf("auto transaction without DB: %v", err)
	}
	if err := m.CreateMany(ctx, []*ExModel{{Name: "auto"}, {ID: 2, Name: "explicit"}}); !errors.Is(err, ErrNilDatabase) {
		t.Errorf("mixed-key batch without DB: %v", err)
	}
	large := make([]*ExModel, bulkInsertMaxRowsPerStmt+1)
	for i := range large {
		large[i] = &ExModel{Name: "row"}
	}
	if err := m.CreateMany(ctx, large); !errors.Is(err, ErrNilDatabase) {
		t.Errorf("chunked batch without DB: %v", err)
	}
	if err := m.BulkInsert(ctx, []*ExModel{{Name: "row"}}); !errors.Is(err, ErrNilDatabase) {
		t.Errorf("bulk insert without DB: %v", err)
	}
	updates := reflect.ValueOf(map[int]string{1: "changed"})
	if err := m.updateManyByKeyChunked(ctx, "id", "name", updates, updates.MapKeys(), 1); !errors.Is(err, ErrNilDatabase) {
		t.Errorf("chunked update without DB: %v", err)
	}
}

func TestResolver_ManualReadRoutingAndExplicitDialects(t *testing.T) {
	primary, replica := openFailureDB(t, &failureDB{}), openFailureDB(t, &failureDB{})
	resolver := &DBResolver{primary: primary, replicas: []*sql.DB{replica}, lb: &RoundRobinLoadBalancer{}}
	m := New[ExModel]()
	m.forcePrimary = true
	if m.resolveDB(resolver) != primary {
		t.Error("primary override ignored")
	}
	m.forcePrimary, m.forceReplica = false, 0
	if m.resolveDB(resolver) != replica {
		t.Error("specific replica override ignored")
	}
	m.forceReplica = 99
	if m.resolveDB(resolver) != replica {
		t.Error("invalid replica should fall back to load balancing")
	}
	m.dialect = DialectSQLite
	q := Query[int]()
	q.dialect = DialectPostgres
	if m.effectiveDialect() != DialectSQLite || q.effectiveDialect() != DialectPostgres {
		t.Error("explicit per-query dialect ignored")
	}
	cache := NewStmtCache(1)
	defer cache.Close()
	m = m.SetDB(primary).WithStmtCache(cache)
	tx, err := primary.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// A transaction's prepare failure must not be hidden by the DB statement cache.
	badDB := openFailureDB(t, &failureDB{prepareErr: errDatabaseFailure})
	badTx, err := badDB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer badTx.Rollback()
	if _, _, err := m.prepareStmtWithQueryer(context.Background(), "SELECT 1", badTx); !errors.Is(err, errDatabaseFailure) {
		t.Errorf("transaction prepare failure lost: %v", err)
	}
	if _, _, err := m.prepareStmtWithQueryer(context.Background(), "SELECT 1", nil); err == nil {
		t.Error("invalid statement queryer accepted")
	}
}

type missingPrimaryModel struct{ Name string }

func TestMissingPrimaryKey_ReturnsErrors(t *testing.T) {
	ctx := context.Background()
	m := New[missingPrimaryModel]().SetDB(openFailureDB(t, &failureDB{queryErr: errDatabaseFailure}))
	for _, operation := range []string{"Create", "Save", "Update", "CreateMany", "BulkInsert"} {
		entity := &missingPrimaryModel{Name: "row"}
		var err error
		switch operation {
		case "Create":
			err = m.Create(ctx, entity)
		case "Save":
			err = m.Save(ctx, entity)
		case "Update":
			err = m.Update(ctx, entity)
		case "CreateMany":
			err = m.CreateMany(ctx, []*missingPrimaryModel{entity})
		case "BulkInsert":
			err = m.BulkInsert(ctx, []*missingPrimaryModel{entity})
		}
		if err == nil || !strings.Contains(err.Error(), "primary key") {
			t.Errorf("%s without primary key: %v", operation, err)
		}
	}
}

func TestCountOver_ValidationAndDriverFailures(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"builder", "column", "prepare", "query", "scan", "iteration"} {
		t.Run(mode, func(t *testing.T) {
			failure := &failureDB{columns: []string{"value", "count"}}
			m := New[ExModel]().SetDB(openFailureDB(t, failure))
			column := "value"
			switch mode {
			case "builder":
				m = m.Select("id;drop")
			case "column":
				column = "value;drop"
			case "prepare":
				failure.prepareErr = errDatabaseFailure
				cache := NewStmtCache(1)
				defer cache.Close()
				m = m.WithStmtCache(cache)
			case "query":
				failure.queryErr = errDatabaseFailure
			case "scan":
				failure.values = [][]driver.Value{{int64(1), "bad count"}}
			case "iteration":
				failure.iterationErr = errDatabaseFailure
			}
			_, err := m.CountOver(ctx, column)
			if err == nil {
				t.Fatal("failure was ignored")
			}
			if (mode == "prepare" || mode == "query" || mode == "iteration") && !errors.Is(err, errDatabaseFailure) {
				t.Errorf("driver error lost: %v", err)
			}
		})
	}
}

func TestDirtyTracking_PartialBaselineTreatsMissingValuesAsDirty(t *testing.T) {
	entity := &ExModel{ID: 1, Name: "A", Value: 10}
	m := New[ExModel]()
	globalTracker.Load().Store(getEntityKey(entity), map[string]any{"id": 1, "name": "A"}, weakRef[ExModel]{p: weak.Make(entity)})
	defer ClearOriginals(entity)
	if !m.IsDirtyField(entity, "value") || m.IsDirtyField(entity, "name") {
		t.Error("partial baseline failed to distinguish missing from unchanged values")
	}
}

func TestClone_OmitColumnsAreIndependent(t *testing.T) {
	original := New[ExModel]().Omit("name")
	clone := original.Clone().Omit("value")
	if original.omitColumns["value"] || !clone.omitColumns["name"] || !clone.omitColumns["value"] {
		t.Error("clone and original share omitted-column state")
	}
}
