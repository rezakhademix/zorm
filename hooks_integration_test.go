package zorm

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// setupHooksDB creates an in-memory SQLite DB for hook tests.
func setupHooksDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	_, err = db.Exec(`
		CREATE TABLE hook_items (
			id    INTEGER PRIMARY KEY,
			name  TEXT,
			value INTEGER
		);
	`)
	if err != nil {
		t.Fatalf("failed to setup hooks DB: %v", err)
	}
	return db
}

// ---- AfterCreate ----

// HookAfterCreateItem implements AfterCreate.
type HookAfterCreateItem struct {
	ID            int `zorm:"primaryKey"`
	Name          string
	Value         int
	AfterCreateOK bool `zorm:"-"` // not stored; set by hook
}

func (h *HookAfterCreateItem) TableName() string { return "hook_items" }

func (h *HookAfterCreateItem) AfterCreate(ctx context.Context) error {
	h.AfterCreateOK = true
	return nil
}

func TestHook_AfterCreate_CalledAfterInsert(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	ctx := context.Background()
	item := &HookAfterCreateItem{Name: "test", Value: 42}

	err := New[HookAfterCreateItem]().SetDB(db).Create(ctx, item)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if item.ID == 0 {
		t.Error("expected ID to be set after Create")
	}
	if !item.AfterCreateOK {
		t.Error("expected AfterCreate to be called, but it was not")
	}
}

// HookAfterCreateError implements AfterCreate that returns an error.
type HookAfterCreateError struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookAfterCreateError) TableName() string { return "hook_items" }

func (h *HookAfterCreateError) AfterCreate(ctx context.Context) error {
	return errors.New("after-create intentional error")
}

func TestHook_AfterCreate_ErrorPropagates(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	ctx := context.Background()
	item := &HookAfterCreateError{Name: "fail", Value: 1}

	err := New[HookAfterCreateError]().SetDB(db).Create(ctx, item)
	if err == nil {
		t.Fatal("expected AfterCreate error to propagate, got nil")
	}
	if !errors.Is(err, errors.New("after-create intentional error")) && err.Error() != "after-create intentional error" {
		t.Errorf("unexpected error: %v", err)
	}
}

// ---- AfterCreate in BulkInsert ----

type HookBulkItem struct {
	ID             int `zorm:"primaryKey"`
	Name           string
	Value          int
	AfterCreateCnt int `zorm:"-"`
}

func (h *HookBulkItem) TableName() string { return "hook_items" }

func (h *HookBulkItem) AfterCreate(ctx context.Context) error {
	h.AfterCreateCnt++
	return nil
}

func TestHook_AfterCreate_BulkInsert(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	ctx := context.Background()
	items := []*HookBulkItem{
		{Name: "a", Value: 1},
		{Name: "b", Value: 2},
		{Name: "c", Value: 3},
	}

	err := New[HookBulkItem]().SetDB(db).BulkInsert(ctx, items)
	if err != nil {
		t.Fatalf("BulkInsert failed: %v", err)
	}
	for i, item := range items {
		if item.AfterCreateCnt != 1 {
			t.Errorf("item[%d]: expected AfterCreate called once, got %d", i, item.AfterCreateCnt)
		}
	}
}

// ---- BeforeDelete ----

var beforeDeleteCalled bool

type HookBeforeDeleteItem struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookBeforeDeleteItem) TableName() string { return "hook_items" }

func (h *HookBeforeDeleteItem) BeforeDelete(ctx context.Context) error {
	beforeDeleteCalled = true
	return nil
}

func TestHook_BeforeDelete_Called(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	// Insert a row directly
	_, err := db.Exec(`INSERT INTO hook_items (id, name, value) VALUES (99, 'del', 10)`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	beforeDeleteCalled = false
	ctx := context.Background()

	err = New[HookBeforeDeleteItem]().SetDB(db).Where("id", 99).Delete(ctx)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if !beforeDeleteCalled {
		t.Error("expected BeforeDelete to be called, but it was not")
	}
}

// HookBeforeDeleteAbort causes delete to abort.
type HookBeforeDeleteAbort struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookBeforeDeleteAbort) TableName() string { return "hook_items" }

func (h *HookBeforeDeleteAbort) BeforeDelete(ctx context.Context) error {
	return errors.New("delete aborted by hook")
}

func TestHook_BeforeDelete_Aborts(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	_, err := db.Exec(`INSERT INTO hook_items (id, name, value) VALUES (100, 'keep', 10)`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	ctx := context.Background()
	err = New[HookBeforeDeleteAbort]().SetDB(db).Where("id", 100).Delete(ctx)
	if err == nil {
		t.Fatal("expected BeforeDelete to abort the delete, got nil")
	}
	if err.Error() != "delete aborted by hook" {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify the row was NOT deleted
	var count int
	db.QueryRow("SELECT COUNT(*) FROM hook_items WHERE id = 100").Scan(&count)
	if count != 1 {
		t.Errorf("expected row to remain after aborted delete, got count=%d", count)
	}
}

// ---- AfterDelete ----

var afterDeleteCalled bool

type HookAfterDeleteItem struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookAfterDeleteItem) TableName() string { return "hook_items" }

func (h *HookAfterDeleteItem) AfterDelete(ctx context.Context) error {
	afterDeleteCalled = true
	return nil
}

func TestHook_AfterDelete_CalledAfterSuccessfulDelete(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	_, err := db.Exec(`INSERT INTO hook_items (id, name, value) VALUES (101, 'bye', 5)`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	afterDeleteCalled = false
	ctx := context.Background()

	err = New[HookAfterDeleteItem]().SetDB(db).Where("id", 101).Delete(ctx)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if !afterDeleteCalled {
		t.Error("expected AfterDelete to be called, but it was not")
	}
}

// ---- AfterFind ----

type HookAfterFindItem struct {
	ID           int `zorm:"primaryKey"`
	Name         string
	Value        int
	AfterFindCnt int `zorm:"-"`
}

func (h *HookAfterFindItem) TableName() string { return "hook_items" }

func (h *HookAfterFindItem) AfterFind(ctx context.Context) error {
	h.AfterFindCnt++
	return nil
}

func TestHook_AfterFind_CalledForEachRow_Get(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	_, err := db.Exec(`
		INSERT INTO hook_items (id, name, value) VALUES
		(1, 'x', 1), (2, 'y', 2), (3, 'z', 3);
	`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	ctx := context.Background()
	results, err := New[HookAfterFindItem]().SetDB(db).Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	for i, r := range results {
		if r.AfterFindCnt != 1 {
			t.Errorf("result[%d]: expected AfterFind called once, got %d", i, r.AfterFindCnt)
		}
	}
}

func TestHook_AfterFind_CalledForFirst(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	_, err := db.Exec(`INSERT INTO hook_items (id, name, value) VALUES (10, 'first', 10)`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	ctx := context.Background()
	result, err := New[HookAfterFindItem]().SetDB(db).Where("id", 10).First(ctx)
	if err != nil {
		t.Fatalf("First failed: %v", err)
	}
	if result.AfterFindCnt != 1 {
		t.Errorf("expected AfterFind called once for First(), got %d", result.AfterFindCnt)
	}
}

// HookAfterFindError makes AfterFind return an error to test propagation.
type HookAfterFindError struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookAfterFindError) TableName() string { return "hook_items" }

func (h *HookAfterFindError) AfterFind(ctx context.Context) error {
	return errors.New("after-find intentional error")
}

func TestHook_AfterFind_ErrorAborts(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	_, err := db.Exec(`INSERT INTO hook_items (id, name, value) VALUES (20, 'err', 20)`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	ctx := context.Background()
	_, err = New[HookAfterFindError]().SetDB(db).Get(ctx)
	if err == nil {
		t.Fatal("expected AfterFind error to propagate from Get(), got nil")
	}
}

func TestHook_AfterFind_CalledInCursor(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	_, err := db.Exec(`
		INSERT INTO hook_items (id, name, value) VALUES
		(30, 'cur1', 1), (31, 'cur2', 2);
	`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	ctx := context.Background()
	cursor, err := New[HookAfterFindItem]().SetDB(db).OrderBy("id", "ASC").Cursor(ctx)
	if err != nil {
		t.Fatalf("Cursor failed: %v", err)
	}
	defer cursor.Close()

	count := 0
	for cursor.Next() {
		item, err := cursor.Scan(ctx)
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if item.AfterFindCnt != 1 {
			t.Errorf("cursor item: expected AfterFind called once, got %d", item.AfterFindCnt)
		}
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 items via cursor, got %d", count)
	}
}

// ---- H1: AfterCreate NOT called when INSERT fails ----

// HookAfterCreateFailItem has AfterCreate that marks itself; used to verify
// that AfterCreate is skipped when the INSERT fails.
type HookAfterCreateFailItem struct {
	ID            int `zorm:"primaryKey"`
	Name          string
	Value         int
	AfterCreateOK bool `zorm:"-"`
}

func (h *HookAfterCreateFailItem) TableName() string { return "hook_items" }

func (h *HookAfterCreateFailItem) AfterCreate(ctx context.Context) error {
	h.AfterCreateOK = true
	return nil
}

func TestHook_AfterCreate_NotCalledWhenInsertFails(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	// Seed a row with id=500 to trigger a PRIMARY KEY conflict.
	_, err := db.Exec(`INSERT INTO hook_items (id, name, value) VALUES (500, 'existing', 10)`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	ctx := context.Background()
	// id=500 is non-zero so it is included in the INSERT; conflict → INSERT fails.
	item := &HookAfterCreateFailItem{ID: 500, Name: "duplicate", Value: 20}
	err = New[HookAfterCreateFailItem]().SetDB(db).Create(ctx, item)
	if err == nil {
		t.Fatal("expected INSERT error due to PRIMARY KEY conflict, got nil")
	}
	if item.AfterCreateOK {
		t.Error("expected AfterCreate NOT to be called when INSERT fails, but it was")
	}
}

// ---- H2: BeforeCreate can modify entity fields before INSERT ----

// HookBeforeCreateModify rewrites Name in BeforeCreate; the modified value
// must be visible in both the returned entity and the persisted row.
type HookBeforeCreateModify struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookBeforeCreateModify) TableName() string { return "hook_items" }

func (h *HookBeforeCreateModify) BeforeCreate(ctx context.Context) error {
	h.Name = "modified"
	return nil
}

func TestHook_BeforeCreate_CanModifyEntity(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	ctx := context.Background()
	item := &HookBeforeCreateModify{Name: "original", Value: 1}

	err := New[HookBeforeCreateModify]().SetDB(db).Create(ctx, item)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// The hook runs before the INSERT query is built, so the mutation is picked up.
	if item.Name != "modified" {
		t.Errorf("expected entity Name='modified' after BeforeCreate, got %q", item.Name)
	}

	// Confirm the modified name was persisted.
	var name string
	if err := db.QueryRow("SELECT name FROM hook_items WHERE id = ?", item.ID).Scan(&name); err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if name != "modified" {
		t.Errorf("expected persisted name='modified', got %q", name)
	}
}

// ---- H3: AfterFind NOT called for an empty result set ----

var afterFindEmptyCalled bool

// HookAfterFindEmptyItem uses a package-level flag so the test can detect
// whether AfterFind fired even though no rows were returned.
type HookAfterFindEmptyItem struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookAfterFindEmptyItem) TableName() string { return "hook_items" }

func (h *HookAfterFindEmptyItem) AfterFind(ctx context.Context) error {
	afterFindEmptyCalled = true
	return nil
}

func TestHook_AfterFind_EmptyResultSet(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	afterFindEmptyCalled = false
	ctx := context.Background()

	results, err := New[HookAfterFindEmptyItem]().SetDB(db).Where("id", 9999).Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
	if afterFindEmptyCalled {
		t.Error("expected AfterFind NOT to be called for empty result set, but it was")
	}
}

// ---- H4: AfterFind can modify the returned entity ----

// HookAfterFindModify overwrites Value in AfterFind; the caller must see the
// modified value because the hook runs before the entity is appended to results.
type HookAfterFindModify struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookAfterFindModify) TableName() string { return "hook_items" }

func (h *HookAfterFindModify) AfterFind(ctx context.Context) error {
	h.Value = 999
	return nil
}

func TestHook_AfterFind_CanModifyReturnedEntity(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	_, err := db.Exec(`INSERT INTO hook_items (id, name, value) VALUES (50, 'modify', 1)`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	ctx := context.Background()
	result, err := New[HookAfterFindModify]().SetDB(db).Where("id", 50).First(ctx)
	if err != nil {
		t.Fatalf("First failed: %v", err)
	}
	// AfterFind sets Value=999; the mutation must be visible to the caller.
	if result.Value != 999 {
		t.Errorf("expected AfterFind to set Value=999, got %d", result.Value)
	}
}

// ---- H5: ForceDeleteAll() triggers BeforeDelete and AfterDelete ----

var forceDeleteBeforeCalled bool
var forceDeleteAfterCalled bool

// HookForceDeleteItem tracks hook invocations via package-level flags so the
// test can verify both hooks fire through the ForceDeleteAll code path.
type HookForceDeleteItem struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookForceDeleteItem) TableName() string { return "hook_items" }

func (h *HookForceDeleteItem) BeforeDelete(ctx context.Context) error {
	forceDeleteBeforeCalled = true
	return nil
}

func (h *HookForceDeleteItem) AfterDelete(ctx context.Context) error {
	forceDeleteAfterCalled = true
	return nil
}

func TestHook_ForceDeleteAll_TriggersHooks(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	_, err := db.Exec(`INSERT INTO hook_items (id, name, value) VALUES (200, 'force', 1)`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	forceDeleteBeforeCalled = false
	forceDeleteAfterCalled = false
	ctx := context.Background()

	err = New[HookForceDeleteItem]().SetDB(db).ForceDeleteAll(ctx)
	if err != nil {
		t.Fatalf("ForceDeleteAll failed: %v", err)
	}
	if !forceDeleteBeforeCalled {
		t.Error("expected BeforeDelete to be called by ForceDeleteAll, but it was not")
	}
	if !forceDeleteAfterCalled {
		t.Error("expected AfterDelete to be called by ForceDeleteAll, but it was not")
	}
}

// ---- H6: AfterDelete NOT called when BeforeDelete returns error ----

var afterDeleteSkipCalled bool

// HookBeforeDeleteAbortAfterCheck returns an error from BeforeDelete and
// records AfterDelete invocations; AfterDelete must be skipped on abort.
type HookBeforeDeleteAbortAfterCheck struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookBeforeDeleteAbortAfterCheck) TableName() string { return "hook_items" }

func (h *HookBeforeDeleteAbortAfterCheck) BeforeDelete(ctx context.Context) error {
	return errors.New("delete aborted by hook")
}

func (h *HookBeforeDeleteAbortAfterCheck) AfterDelete(ctx context.Context) error {
	afterDeleteSkipCalled = true
	return nil
}

func TestHook_AfterDelete_NotCalledWhenBeforeDeleteAborts(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	_, err := db.Exec(`INSERT INTO hook_items (id, name, value) VALUES (300, 'skip', 1)`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	afterDeleteSkipCalled = false
	ctx := context.Background()

	err = New[HookBeforeDeleteAbortAfterCheck]().SetDB(db).Where("id", 300).Delete(ctx)
	if err == nil {
		t.Fatal("expected BeforeDelete to abort delete, got nil")
	}
	if afterDeleteSkipCalled {
		t.Error("expected AfterDelete NOT to be called when BeforeDelete aborts, but it was")
	}
}

// ---- H7: Hooks work inside WithTx() transactions ----

var hookTxAfterCreateCalled bool
var hookTxBeforeDeleteCalled bool
var hookTxAfterDeleteCalled bool

// HookTxItem implements AfterCreate (on the entity) and BeforeDelete/AfterDelete
// (on a zero-value instance per execDelete semantics); all three hooks set
// package-level flags so the test can verify they fired inside a transaction.
type HookTxItem struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookTxItem) TableName() string { return "hook_items" }

func (h *HookTxItem) AfterCreate(ctx context.Context) error {
	hookTxAfterCreateCalled = true
	return nil
}

func (h *HookTxItem) BeforeDelete(ctx context.Context) error {
	hookTxBeforeDeleteCalled = true
	return nil
}

func (h *HookTxItem) AfterDelete(ctx context.Context) error {
	hookTxAfterDeleteCalled = true
	return nil
}

func TestHook_Hooks_WorkInTransaction(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	hookTxAfterCreateCalled = false
	hookTxBeforeDeleteCalled = false
	hookTxAfterDeleteCalled = false

	ctx := context.Background()
	model := New[HookTxItem]().SetDB(db)

	err := model.Transaction(ctx, func(tx *Tx) error {
		item := &HookTxItem{Name: "tx-item", Value: 42}
		if err := model.WithTx(tx).Create(ctx, item); err != nil {
			return err
		}
		// Delete the just-created row within the same transaction.
		return model.WithTx(tx).Where("id", item.ID).Delete(ctx)
	})
	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}

	if !hookTxAfterCreateCalled {
		t.Error("expected AfterCreate to be called within transaction, but it was not")
	}
	if !hookTxBeforeDeleteCalled {
		t.Error("expected BeforeDelete to be called within transaction, but it was not")
	}
	if !hookTxAfterDeleteCalled {
		t.Error("expected AfterDelete to be called within transaction, but it was not")
	}

	// After commit, the insert+delete are permanent; the table must be empty.
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM hook_items").Scan(&count); err != nil {
		t.Fatalf("COUNT query failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows after committed transaction (create+delete), got %d", count)
	}
}
