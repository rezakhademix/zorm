package zorm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestFinalBoundaries_EmptyWritesAndRejectedCursorOptions(t *testing.T) {
	ctx := context.Background()
	for _, configure := range []func(*Model[ExModel]) *Model[ExModel]{
		func(m *Model[ExModel]) *Model[ExModel] { return m.With("Children") },
		func(m *Model[ExModel]) *Model[ExModel] { return m.WithCallback("Children", nil) },
		func(m *Model[ExModel]) *Model[ExModel] { return m.WithMorph("Subject", nil) },
	} {
		if _, err := configure(New[ExModel]()).Cursor(ctx); err == nil {
			t.Error("cursor accepted eager loading")
		}
	}
	m := New[ExModel]()
	if err := m.Delete(ctx); !errors.Is(err, ErrInvalidModel) {
		t.Errorf("unguarded delete accepted: %v", err)
	}
	if err := m.UpdateMany(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := New[caOnly]().UpdateMany(ctx, map[string]any{"created_at": time.Time{}}); err != nil {
		t.Fatal(err)
	}
	if err := m.BulkInsert(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := m.bulkInsertGroup(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got := formatArgs([]any{strings.Repeat("x", 300)}); len(got) != 200 || !strings.HasSuffix(got, "...]") {
		t.Errorf("unbounded error arguments: %q", got)
	}
	var builder Model[ExModel]
	builder.WithCallback("Children", func(*Model[ExModel]) {}).WithMorph("Subject", map[string][]string{"child": {"Children"}})
	if builder.relationCallbacks["Children"] == nil || len(builder.morphRelations["Subject"]) != 1 {
		t.Error("zero-value relation option maps were not initialized")
	}
}

func TestFinalBoundaries_PaginationAndLegacyDialect(t *testing.T) {
	db := setupExDB(t)
	defer db.Close()
	ctx := context.Background()
	page, err := New[ExModel]().SetDB(db).Where("id", 999).Paginate(ctx, 1, 2)
	if err != nil || page.Total != 0 || page.LastPage != 1 || len(page.Data) != 0 {
		t.Fatalf("empty pagination: %+v %v", page, err)
	}
	m := New[ExModel]().SetDB(db).addWhere("AND", "name", "A").Oldest("id")
	if rows, err := m.Get(ctx); err != nil || len(rows) != 1 || rows[0].Name != "A" {
		t.Fatalf("implicit comparison/oldest failed: %v %v", rows, err)
	}
	old := GetDialect()
	SetDialect(DialectAuto)
	defer SetDialect(old)
	RegisterDriverDialect("*sqlite3.SQLiteDriver", DialectAuto)
	defer RegisterDriverDialect("*sqlite3.SQLiteDriver", DialectSQLite)
	if detectDialect(db) != DialectSQLite {
		t.Error("unregistered SQLite driver lost legacy detection")
	}
	oldResolver := GetGlobalResolver()
	defer globalResolver.Store(oldResolver)
	replica := openFailureDB(t, &failureDB{})
	globalResolver.Store(&DBResolver{primary: db, replicas: []*sql.DB{replica}, lb: &RoundRobinLoadBalancer{}})
	if Query[int]().queryer() != replica {
		t.Error("scalar read did not route to replica")
	}
}

func TestFinalBoundaries_BatchHelpersAndCallerTransaction(t *testing.T) {
	ctx := context.Background()
	db := setupExDB(t)
	defer db.Close()
	m := New[ExModel]().SetDB(db)
	err := m.Transaction(ctx, func(tx *Tx) error {
		rows := []*ExModel{{Name: "generated"}, {ID: 100, Name: "explicit"}}
		if err := m.WithTx(tx).CreateMany(ctx, rows); err != nil {
			return err
		}
		if rows[0].ID == 0 || rows[1].ID != 100 {
			t.Error("mixed batch lost keys")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, fields := m.insertColumns([]*ExModel{nil, {ID: 1}}, false)
	if len(fields) != 3 {
		t.Error("nil entries prevented insert column discovery")
	}
	auto, explicit, mixed := m.partitionByAutoPK([]*ExModel{nil, {}, {ID: 1}})
	if !mixed || len(auto) != 2 || len(explicit) != 1 {
		t.Error("nil/zero/explicit partition is inconsistent")
	}
	extra := New[ExModel]().SetDB(openFailureDB(t, &failureDB{columns: []string{"id"}, values: [][]driver.Value{{int64(1)}, {int64(2)}}}))
	row := &ExModel{Name: "one"}
	if err := extra.CreateMany(ctx, []*ExModel{row}); err != nil || row.ID != 1 {
		t.Errorf("extra result rows overwrote target or panicked: %v %v", row, err)
	}
}

func TestFinalBoundaries_MixedBatchPanicRollsBack(t *testing.T) {
	rollbacks := 0
	m := New[ExModel]().SetDB(openFailureDB(t, &failureDB{query: func(string) (driver.Rows, error) {
		return &panicBatchRows{failureRows{columns: []string{"id"}}}, nil
	}, onRollback: func() { rollbacks++ }}))
	defer func() {
		if got := recover(); got != "driver panic" || rollbacks != 1 {
			t.Errorf("panic not propagated or transaction leaked: %v rollbacks=%d", got, rollbacks)
		}
	}()
	_ = m.CreateMany(context.Background(), []*ExModel{{Name: "auto"}, {ID: 100, Name: "explicit"}})
}

type panicBatchRows struct{ failureRows }

func (*panicBatchRows) Next([]driver.Value) error { panic("driver panic") }

type createAfterReject struct {
	ID   int
	Name string
}

func (*createAfterReject) AfterCreate(context.Context) error { return errDatabaseFailure }

func TestFinalBoundaries_BulkAfterHookFailure(t *testing.T) {
	m := New[createAfterReject]().SetDB(openFailureDB(t, &failureDB{columns: []string{"id"}, values: [][]driver.Value{{int64(1)}}}))
	if err := m.BulkInsert(context.Background(), []*createAfterReject{{Name: "A"}}); !errors.Is(err, errDatabaseFailure) {
		t.Errorf("bulk after hook error lost: %v", err)
	}
}

func TestFinalBoundaries_MixedBulkStopsAfterFirstGroupFailure(t *testing.T) {
	queries := 0
	m := New[ExModel]().SetDB(openFailureDB(t, &failureDB{query: func(string) (driver.Rows, error) {
		queries++
		return nil, errDatabaseFailure
	}}))
	rows := []*ExModel{{Name: "generated"}, {ID: 100, Name: "explicit"}}
	if err := m.BulkInsert(context.Background(), rows); !errors.Is(err, errDatabaseFailure) || queries != 1 {
		t.Errorf("first group failure was ignored or explicit group was attempted: queries=%d err=%v", queries, err)
	}
	if rows[0].ID != 0 || rows[1].ID != 100 {
		t.Error("failed bulk insert changed primary keys")
	}
}

type fallbackPrimaryParent struct {
	ID       int
	Children []*failureChild
	Photos   []*failureChild
}

func (fallbackPrimaryParent) PrimaryKey() string { return "absent" }
func (fallbackPrimaryParent) MorphType() string  { return "failureParent" }
func (fallbackPrimaryParent) ChildrenRelation() HasMany[failureChild] {
	return HasMany[failureChild]{ForeignKey: "parent_id"}
}
func (fallbackPrimaryParent) PeersRelation() BelongsToMany[failureChild] {
	return (failureParent{}).PeersRelation()
}

func TestFinalBoundaries_ConventionalKeyRelationFallback(t *testing.T) {
	ctx := context.Background()
	db := setupFailureRelations(t)
	m := New[fallbackPrimaryParent]().SetDB(db)
	parent := &fallbackPrimaryParent{ID: 1}
	for _, dynamic := range []bool{false, true} {
		hasMany := parent.ChildrenRelation()
		morph := MorphMany[failureChild]{Type: "owner_type", ID: "owner_id", Table: "failure_children"}
		var err error
		if dynamic {
			err = m.loadHasManyDynamic(ctx, []any{parent}, reflect.TypeFor[fallbackPrimaryParent](), &hasMany, "Children", "", nil)
			if err == nil {
				err = m.loadMorphOneOrManyDynamic(ctx, []any{parent}, reflect.TypeFor[fallbackPrimaryParent](), &morph, "Photos", "", nil)
			}
		} else {
			err = m.loadHasMany(ctx, []*fallbackPrimaryParent{parent}, &hasMany, "Children", "", nil, nil)
			if err == nil {
				err = m.loadMorphOneOrMany(ctx, []*fallbackPrimaryParent{parent}, &morph, "Photos", "", nil, &relationConstraints{wheres: []string{"AND name = ?"}, args: []any{"child"}})
			}
		}
		if err != nil || len(parent.Children) != 1 || len(parent.Photos) != 1 {
			t.Fatalf("conventional key fallback failed: %v", err)
		}
	}
	if err := m.Attach(ctx, parent, "Peers", []any{2, 3}, nil); err != nil {
		t.Fatal(err)
	}
	if err := m.Detach(ctx, parent, "Peers", []any{2}); err != nil {
		t.Fatal(err)
	}
	if err := m.Sync(ctx, parent, "Peers", []any{1, 3}, nil); err != nil {
		t.Fatal(err)
	}
}
