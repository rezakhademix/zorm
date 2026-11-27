package zorm

import (
	"context"
	"database/sql/driver"
	"errors"
	"reflect"
	"testing"
)

type createGuardItem struct {
	ID   int
	Name string
}

func (*createGuardItem) BeforeCreate(context.Context) error { return errDatabaseFailure }

func TestWriteGuards_HookRejectionsAndAutoTxBeginFailure(t *testing.T) {
	ctx := context.Background()
	m := New[createGuardItem]().SetDB(openFailureDB(t, &failureDB{}))
	if err := m.Create(ctx, &createGuardItem{Name: "A"}); !errors.Is(err, errDatabaseFailure) {
		t.Errorf("Create ignored before hook: %v", err)
	}
	if err := m.BulkInsert(ctx, []*createGuardItem{{Name: "A"}}); !errors.Is(err, errDatabaseFailure) {
		t.Errorf("BulkInsert ignored before hook: %v", err)
	}
	for _, before := range []bool{false, true} {
		save := New[partialHookItem]().SetDB(openFailureDB(t, &failureDB{affected: 1}))
		entity := &partialHookItem{ID: 1, Name: "A"}
		trackOriginals(entity, save.modelInfo)
		defer ClearOriginals(entity)
		if before {
			entity.Before = func(*partialHookItem) error { return errDatabaseFailure }
		} else {
			entity.After = func(*partialHookItem) error { return errDatabaseFailure }
		}
		entity.Name = "B"
		if err := save.Save(ctx, entity); !errors.Is(err, errDatabaseFailure) {
			t.Errorf("Save ignored hook rejection: %v", err)
		}
	}
	auto := New[ExModel]().SetDB(openFailureDB(t, &failureDB{beginErr: errDatabaseFailure}))
	if err := auto.withAutoTx(ctx, func(*Model[ExModel]) error { t.Error("callback after begin failure"); return nil }); !errors.Is(err, errDatabaseFailure) {
		t.Errorf("auto transaction lost begin error: %v", err)
	}
}

func TestCursorGuards_ClosedRowsAndRejectedReadHook(t *testing.T) {
	ctx := context.Background()
	m := New[failureChild]().SetDB(openFailureDB(t, &failureDB{columns: []string{"id", "name"}, values: [][]driver.Value{{int64(1), "reject"}}}))
	cursor, err := m.Cursor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if !cursor.Next() {
		t.Fatal("missing row")
	}
	if _, err := cursor.Scan(ctx); !errors.Is(err, errDatabaseFailure) {
		t.Errorf("cursor ignored read hook: %v", err)
	}
	closed, err := m.Cursor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	closed.Close()
	if _, err := closed.Scan(ctx); err == nil {
		t.Error("closed cursor scan succeeded")
	}
}

func TestChunkedKeyUpdates_TransactionFailures(t *testing.T) {
	ctx := context.Background()
	values := reflect.ValueOf(map[int]string{1: "A", 2: "B", 3: "C"})
	for _, mode := range []string{"begin", "execute", "commit"} {
		failure := &failureDB{}
		switch mode {
		case "begin":
			failure.beginErr = errDatabaseFailure
		case "execute":
			failure.execErr = errDatabaseFailure
		case "commit":
			failure.commitErr = errDatabaseFailure
		}
		m := New[ExModel]().SetDB(openFailureDB(t, failure))
		if err := m.updateManyByKeyChunked(ctx, "id", "name", values, values.MapKeys(), 2); !errors.Is(err, errDatabaseFailure) {
			t.Errorf("chunked key update lost %s failure: %v", mode, err)
		}
	}
}

type attributesWithoutAccessors struct {
	ID         int
	Attributes map[string]any `zorm:"-"`
}

func TestCacheAndAccessorGuards_EmptyAndDefaultStates(t *testing.T) {
	for _, capacity := range []int{0, -1} {
		cache := NewStmtCache(capacity)
		if cache.capacity != 100 {
			t.Error("invalid default capacity")
		}
		cache.shards[0].mu.Lock()
		cache.evictLRU(cache.shards[0])
		cache.shards[0].mu.Unlock()
		cache.Close()
	}
	columns := newColumnCache(0)
	columns.Store(columnTestKey("one"), nil)
	if _, ok := columns.Load(columnTestKey("one")); !ok {
		t.Error("minimal column cache is unusable")
	}
	tracker := newLRUTracker(1)
	shard := tracker.shards[0]
	shard.mu.Lock()
	shard.evictLRU()
	shard.mu.Unlock()
	if tracker.Len() != 0 {
		t.Error("empty eviction changed tracker size")
	}
	m := New[attributesWithoutAccessors]()
	entity := &attributesWithoutAccessors{ID: 1}
	loadAccessorsInfo([]any{entity}, m.modelInfo)
	m.loadAccessorsSingle(entity)
	m.loadAccessorsSingle(nil)
	if entity.Attributes != nil {
		t.Error("no accessor should leave attributes unchanged")
	}
	if !isZeroPK(nil) || !isZeroCreatedAt(nil) {
		t.Error("nil values should be recognized as zero")
	}
	if needsAutoTx(writeOp(0), &ExModel{}) {
		t.Error("unknown operation requested an automatic transaction")
	}
}
